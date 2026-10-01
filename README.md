# BitRobot stereo rectification

`bitrobot.py` is the BitRobot (RoboCap) adapter of a stereo pipeline's normalization
stage. It turns one raw recording chunk (six fisheye cameras plus IMU) into a standard
package: raw and rectified videos, calibration, frame timestamps and IMU. This version
fixes the rectification and adds a built-in rectification QC that writes
`rectification_qc.json` next to the package.

The file imports the pipeline's own packages (`common`, `normalization`), so it is
published for reference, not as a standalone tool.

## The problem

The old rectification built each stereo pair's rectified camera by hand (a Bouguet-style
split rotation) at a **fixed 114° horizontal field**. 114° is chosen so a 1920×1080
pinhole meets the required field of view of **at least 110° horizontal and 80°
vertical** (114° gives 114.0° × 81.8°).

A fixed field ignores where the lens calibration is valid. With a poor calibration (for
example a fisheye polynomial that folds back before the image edge), the 114° view
samples outside the usable image. Those pixels come out black, as **black borders**
covering up to about 30% of the frame, and the frame edges are geometrically wrong.

## How rectification is fixed

1. **OpenCV camera.** `cv2.fisheye.stereoRectify` (balance 0, `CALIB_ZERO_DISPARITY`)
   gives the rectifying rotations and a camera that covers only pixels the lens sees.
2. **Cover zoom.** Balance 0 can still leave thin black slivers: OpenCV sizes the camera
   from mid-edge points, keeps the smaller focal length of the two eyes and averages
   their principal points. The focal length is raised by the smallest zoom that keeps
   every output pixel at least 2 px inside the raw image (the reach of the bicubic
   remap), verified on every pixel. The zoom is capped at 1.5×.
3. **Field-of-view check.** The covered camera must still meet 110° × 80°.
4. **Fallback.** The original fixed-field construction (`rectified_hfov_degrees`, 114°)
   is used when the OpenCV camera is unusable: a degenerate focal length, an
   implausible field (outside 30–170°), a principal point outside the frame, a
   non-positive baseline, or a field below 110° × 80°.
5. **Published geometry.** `calibration.json` carries exactly the camera that built the
   videos (rectified intrinsics, R1/R2), so downstream geometry matches the pixels.

| Calibration | Result |
| --- | --- |
| Good | OpenCV camera: no border, field wider than before (114–123° H in tests) |
| Poor, OpenCV field ≥ 110° × 80° | OpenCV camera: no border |
| Poor, OpenCV field below spec, or OpenCV fails | Fixed 114° camera: spec met, border may remain and the QC fails it |

## How the QC validation works

The QC runs after every video in the package is written and decodes the delivered
files, so it judges what downstream receives. It never fails the chunk: if the QC
itself breaks, the file records a failed result instead.

**Sampling.** Five frames per video, at 8, 28, 50, 72 and 92% of the frame count. Frames
are addressed by **exact index**: each seek goes to the midpoint between the
presentation times of frame i−1 and frame i, so raw streams with jittery timestamps
cannot return a neighbouring frame. A raw video and its rectified video, and the two
eyes of a pair, are always compared on the same captures.

The checks run in this order:

1. **Border.** Using the per-pixel maximum over B, G and R, a pixel is a candidate if it
   is at most 16 in every sampled frame (fill cannot move). A candidate region must
   touch a frame edge (fill is anchored to one), and counts as fill only when at least
   85% of it is at most 8, near absolute black (dim walls and vignetted corners sit
   between 9 and 16). **Fail** above 1.5% of the frame.
2. **Distortion.**
   - *Lens model:* over exactly the angles the remap samples, the model must not fold
     back on itself, must stay between orthographic and rectilinear (±2%), and a
     fisheye must stay within 10% of the stereographic bound.
   - *Remap consistency:* raw frames are re-rectified from the shipped calibration and
     compared with the delivered frames using ORB feature matches. The 90th-percentile
     offset must be at most 3 px.
   - **Fail** if the lens model is invalid or the remap disagrees.
3. **Epipolar** (stereo pairs only, **recorded, never judged**). ORB features with
   Lowe's ratio test and mutual matching, a robust inlier band whose scale cannot
   collapse to zero, and vertical-disparity median, RMS and p95. A Huber-weighted plane
   fit `dy = a·u + b·v + c` separates systematic misalignment from noise.

**Verdict.** The chunk passes when `left_rectified.mp4` and `right_rectified.mp4` both
pass border and distortion. The eye and far streams get the same checks and their own
verdicts, recorded but not counted. A check that cannot be measured counts as a fail.

**`rectification_qc.json`.** Top-level keys, in order: `verdict`, `reason`,
`verdict_rule`, `main_videos`, `other_videos`, `epipolar`, `rectification` (per pair:
method, fallback reason, cover zoom, per-eye field and lens checks), `not_checked`,
`thresholds`, `sample_frame_fractions`, `elapsed_s`.

**Cost.** About 10 s per 10-minute chunk on a laptop CPU, with a peak of about 230 MB
after encoding has finished. No GPU is used.

## Results

Twenty devices, each on one production chunk, compare the old calibration with the old
code against the new calibration with this code:

| | Old | New |
| --- | --- | --- |
| Devices with a black border over 1.5% | 4 | 0 |
| Worst border | 30.7% | 0.00% |
| QC pass (core pair) | 15 of 20 | 20 of 20 |
| Lens model valid on both eyes | 16 of 20 | 20 of 20 |
| Rows aligned (median under 1 px, not part of the verdict) | 12 of 18 | 3 of 18 |

On this older footage, row alignment is worse with the new calibrations. Their
camera-to-camera rotation does not match recordings made before the calibration, and
the pipeline's automatic correction is capped at 3°. Check footage recorded after the
new calibration before relying on it for stereo depth.

## Compatibility

No existing output changes structure: `chunk_report.json`, `calibration*.json`,
`meta.json` and `sync.json` keep their keys. The only new file is
`rectification_qc.json`.
