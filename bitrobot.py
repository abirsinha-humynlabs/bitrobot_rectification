"""RoboCap (frodobots) adapter for stereo-pipeline-v2 normalization.

Recovery-oriented. Adds, on top of prior timestamp association and
coherence gating:

  * extrinsic refinement sampled from ASSOCIATED frame indices, not wall time
    (v1 samples both eyes at the same file-relative msec; the two files do not
    start at the same instant on 24.8% of pairs, so the fit was absorbing ~1 deg
    of head motion into the rig extrinsic. refine_extrinsics: true is set for
    bitrobot in pipeline.yaml, so this is live, not latent.)
  * hot-journal / malformed IMU salvage via `sqlite3 .recover`
  * CORE-PAIR ELECTION eye -> front, so a segment whose eye pair straddles two
    recordings still ships on its calibrated front pair
  * the IMU-uncovered head is KEPT, not dropped, and published as
    grade.timing.imu_covered_frame_range — v1 dropped it, which cost the raw
    stream-copy path on ~81% of chunks against a documented "never re-encoded"
    contract
  * a union break primitive over {video gaps, IMU gaps, gyro saturation}
  * an IMU-pair residual gate that catches gyro AXIS SWAPS, which the
    gravity-based scale fit cannot see
  * meta.grade: a machine-readable delivery tier so video+IMU deliverables are
    not blocked by VIO-grade criteria

Original module docstring follows.

RoboCap (frodobots) camera adapter for stereo-pipeline-v2 normalization.

Converts a RoboCap recording chunk to the Standard Package v2.

Consumed input files (raw/bitrobot/<project>/<date>/<unit>/<session>/<chunk>/),
2026-08 upload layout:
    ├── calibration.json          # CORE stereo pair + IMU  (trigger file)
    ├── left_XXX.mp4,  right_XXX.mp4           # core pair (mandatory)
    ├── calibration_eye.json                   # optional "eye" aux pair
    ├── left_eye_XXX.mp4, right_eye_XXX.mp4
    ├── calibration_left_far.json, calibration_right_far.json
    │                         # per-CAMERA Kalibr files: left_far/right_far are
    │                         # independent MONO streams, NOT a stereo pair
    ├── left_far_XXX.mp4,  right_far_XXX.mp4
    ├── imu_XXX.db            # imuid_=1, the CALIBRATED IMU the calib describes
    ├── imu_left_XXX.db       # imuid_=2, the UNCALIBRATED second IMU (optional)
    └── mag_middle_XXX.db     # magnetometer, no slot in the v2 schema

Pre-2026-08 layout (still normalizes — PAIRS carries both aux naming schemes
and find_chunk resolves the IMU filenames per layout):
    calibration_front.json + left_front/right_front_XXX.mp4 for the aux pair,
    imu_right_XXX.db = calibrated IMU, imu_XXX.db = uncalibrated second IMU.
Only calibration.json + left/right_XXX.mp4 + the calibrated IMU are mandatory;
every other pair/calibration/sensor is optional — present means processed,
absent means skipped (calibration absent on a present pair = raw-only + a
MISSING_CALIBRATION_<pair>.md marker).

THE RIG. RoboCap carries THREE stereo pairs, not one. The v2 Standard Package
describes exactly ONE stereo pair. This adapter emits ONE package per chunk:
whichever pair the recorder uploads as plain left_XXX/right_XXX (the CORE pair)
fills the core 11-file contract at the package root (left_raw.mp4, ...,
calibration.json, imu.csv, frame_timestamps.csv, meta.json —
validate_package() passes unchanged, which is what keeps every downstream
stage working unmodified), and the other pairs (eye/front/far, whichever ship)
ride along as AUX STREAMS with suffixed names, declared in meta.json's
aux_streams block (docs/06-bitrobot-device.md). Only normalization and
chunking ever touch the aux files:

    left_<pair>_raw.mp4  right_<pair>_raw.mp4        (+ *_rectified.mp4 when
    calibration_<pair>.json  frame_timestamps_<pair>.csv    calibrated)
    imu_secondary.csv (+ README)  sync.json  chunk_report.json
    rectification_qc.json     # QC verdict (core pair) + every rectified video's checks

Aux pairs get NO sbs composite (57% of the bytes, nothing consumes it) and
NO separate meta — the single root meta.json + chunk_report.json carry the
per-pair facts. A present-but-uncalibrated pair is emitted raw-only and marked
with MISSING_CALIBRATION_<pair>.md so nothing downstream mistakes it for a
full package. left_far/right_far are independent MONO streams (vendor-
confirmed): each is published raw with its own frame-timestamps file, an
UNDISTORTED (pinhole) video built from its own calibration, and a
calibration file wrapping the vendor doc + the pinhole intrinsics — no
stereo association (MONO_STREAMS / _build_mono_stream).

CHOOSING THE IMU. The recorder writes two 6-DoF IMUs; only one is the sensor
the calibration's imu block ("id": "imu0", "rostopic": "/imu_right_1")
actually describes — imuid_=1 inside the sqlite, enforced by load_imu.
T_cam0_imu, the noise model and the cam-IMU timeshift are all in THAT
sensor's frame; pairing them with the other sensor (imuid_=2, ~179 deg
rotated and ~129 mm away per the calibration's own provenance) would make
every VIO solve confidently wrong. Filenames differ by layout (see above);
find_chunk resolves them and the imuid_ guard verifies the pick either way.
imu.csv is built from the calibrated sensor only.

FRAME <-> IMU SYNC. There are no fsync markers here (that was an akai
mechanism). Instead every mp4 carries its capture start on the SAME free-running
device clock as the IMU, in MICROSECONDS, in the container `comment` tag. Frame
timestamps are therefore comment/1e6 + container PTS, and imu.csv and
frame_timestamps.csv are rebased against one shared origin so the two line up.

KNOWN LIMITS (measured, not speculative -- see the IMU audit):
- Chunk-boundary IMU hole. Trimming the accelerometer to the gyro's span drops
  the ~3 leading accel samples that precede the first gyro sample, costing
  ~14.9 ms at each chunk head. Chunks are contiguous slices of ONE session
  (chunk N+1 starts exactly one sample period after chunk N), so a VIO run
  spanning chunks preintegrates across that hole. Fixing it properly means
  resampling at session level and then slicing -- a stage-shape change, not an
  adapter change. Until then `sync.json` reports `imu_head_trim_s` per chunk.
- Gyro bias (~0.5 dps/axis) is deliberately NOT removed: OKVIS and DROID-VI
  estimate it in-state, and this recording cannot pin it down anyway. Do not
  add a second bias correction downstream.
- The IMU noise model in calibration.json is INHERITED from a different unit
  (see _provenance), not measured for this device.
- The magnetometer is dropped: |m| swings 24.6% over the recording (masonry and
  rebar in shot), no VIO stage here consumes it, and the v2 schema has no slot.
- Clipped-accel shocks land at t ~= 1.6 s and ~= 3.3 s of the sample chunk, i.e.
  inside the IMU-only lead-in; VIO initialization should start after t ~= 4 s.

ENCODING. The source is already H.265 1080p (~4 Mbit/s). The raw trio is
STREAM-COPIED, never re-encoded -- transcoding it would add generation loss and
buy nothing. Only the rectified trio is encoded, because its pixels genuinely
change. Downscaling only ever engages above 1080p; this device is exactly 1080p
so it never triggers today, but a future 4K RoboCap gets handled.
"""

from __future__ import annotations

import json
import logging
import math
import re
import shutil
import sqlite3
import struct
import subprocess
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path

import cv2
import numpy as np

from common.metrics import ExecutionTimer
from normalization import calibration_validation
from normalization.adapters.akai import AkaiCalibration, load_akai_calibration
from normalization.adapters.base import (
    DeviceAdapter,
    NormalizationConfig,
    NormalizationResult,
)
from normalization.adapters.registry import register_adapter
from normalization.schema import (
    SCHEMA_VERSION,
    CalibrationJson,
    CalibrationSection,
    CameraCalibration,
    DeviceInfo,
    EncodingInfo,
    ImuCalibration,
    ImuSample,
    MetaJson,
    ProcessingMetrics,
    RecordingInfo,
    SourceInfo,
    StandardPackage,
    StereoInfo,
    validate_package,
    write_frame_timestamps_csv,
    write_imu_csv,
)

# ── IMU: raw ICM-42688-P counts -> SI ────────────────────────────────────────
# Scales come from calibration.json _provenance.raw_sensor_scales and imply the
# part is running +/-4 g and +/-500 dps.
#
# Evidence the accel scale is right: the accelerometer rails at exactly +32767
# counts, and 32767 / 8192 = 4.000 g -- the full-scale setting is therefore
# +/-4 g and 8192 LSB/g is exact, independent of any gravity measurement.
# Evidence the gyro scale is right: _gyro_scale_factor() below fits a
# multiplier against the accelerometer's gravity rotation on every load. It
# returns ~0.90 for 65.5 LSB/dps and rejects both neighbouring full-scale
# settings outright (0.45x for 32.8, 1.80x for 131). That check runs in
# production, so the claim is re-verified on every chunk rather than asserted
# once here.
#
# NOT evidence: static |a|. There is no static segment in this data (the
# calibration's own _provenance says so). Quiet-window |a| comes out ~9.773,
# 0.35% under 9.80665, but the sign of that offset flips with the window
# selection rule, and BOTH physically independent IMUs on the device report the
# same 9.773 -- two separate parts do not share a trim error, so this is local
# gravity or an LSB convention, not a per-unit scale error to correct.
G = 9.80665
DEFAULT_ACCEL_LSB_PER_G = 8192.0
DEFAULT_GYRO_LSB_PER_DPS = 65.5
# 16-bit part railing at +/-32767. The threshold sits at 3.906 g rather than
# just under the rail because a clipped impact rings: on the sample chunk the
# rail hit (+32767) is immediately followed by -32632, part of the SAME clipped
# transient, which a 32700 threshold silently misses.
SATURATION_COUNT = 32000

# The recorder's OTHER 6-DoF IMU (imu_XXX.db, imuid_=2). It is NOT described by
# any shipped calibration -- no T_cam_imu, no noise model -- so it cannot be
# fused with the cameras today and must never be mistaken for the primary. It is
# still published, on the same rebased clock, because it is real 200 Hz data
# that the raw upload would otherwise strand: it carries a different noise
# realisation (useful for averaging), it sits ~117 mm away (so the pair observes
# angular acceleration directly, which one IMU cannot), and it is a fallback if
# the primary saturates -- the primary already clips at +/-4 g on this chunk.
SECONDARY_IMUID = 2

# A single dropped sample shows up as a 2x gap. A 3x threshold therefore cannot
# fire for any single-sample dropout -- observed max/median on this device is
# only 1.09x, so 1.5x is comfortably above the noise and below one lost sample.
DROPOUT_GAP_FACTOR = 1.5

# Seconds to wait for an encoder to flush and write its trailer after stdin
# closes. Generous, but bounded -- an unattended 51-chunk batch must not hang.
ENCODER_FLUSH_TIMEOUT_S = 600

# Rectified output is a PINHOLE camera and the source is a ~150 deg fisheye. A
# pinhole cannot represent that field (tan blows up at 180 deg), so some has to
# be traded away.
#
# 114 deg is chosen to satisfy 2.4: >=110 deg H and >=80 deg V. The vertical is
# not free -- on 1920x1080 it follows from the horizontal, and 110 deg H gives
# only 77.55 deg V. 114 gives 81.80.
#
# NOTE: this is only the FALLBACK. The value actually used comes from
# stages.normalization.devices.bitrobot.rectified_hfov_degrees in
# config/pipeline.yaml (see the getattr at the build_rect_maps call site), so
# changing this constant alone has no effect on a real run.
DEFAULT_RECTIFIED_HFOV_DEG = 114.0

# 2.1: "Maximum GOP/keyframe interval of 30 frames at 30 fps". Applied to every
# encoded stream by _encoder_args; see the note there for why B-frames are
# disabled alongside it.
GOP_MAX_FRAMES = 30

# Median |accel| over a whole chunk must land near 1 g. Anything outside this
# band means the counts->SI scale is wrong, which is the exact silent failure
# that ships a plausible-looking imu.csv full of wrong numbers.
ACCEL_MAGNITUDE_SANITY_MS2 = (7.0, 13.0)

# A stereo extrinsic COMPOSED from two independent mono cam-IMU calibrations
# (load_composed_calibration) is only usable when the two cameras actually
# share a view. Measured on the 2026-08 vendor files, the far cameras sit
# ~179 deg apart about the vertical axis — outward-facing, zero overlap —
# and rectifying that geometry would emit garbage that LOOKS like a stereo
# pair. Beyond this optical-axes divergence the pair degrades to raw-only.
MAX_COMPOSED_AXES_ANGLE_DEG = 45.0

def _artifact_name(pair: str, artifact: str) -> str:
    """Package filename for one pair's artifact, per the aux-stream contract.

    The CORE pair (the recorder's plain left/right streams) fills the core v2
    names unprefixed; every other pair is an aux stream with suffixed names so
    all of them coexist in one package directory that every existing stage can
    read blind.
    """
    if pair == "core":
        return artifact
    if artifact == "calibration.json":
        return f"calibration_{pair}.json"
    if artifact == "frame_timestamps.csv":
        return f"frame_timestamps_{pair}.csv"
    if artifact == "MISSING_CALIBRATION.md":
        return f"MISSING_CALIBRATION_{pair}.md"
    stem, _, ext = artifact.rpartition(".")
    eye_side, _, kind = stem.partition("_")   # left_raw -> left, raw
    return f"{eye_side}_{pair}_{kind}.{ext}"


# Stereo pairs on the rig: (package name, left stem, right stem, calibration file).
# "core" is whatever the recorder uploads as plain left/right — it fills the
# unsuffixed v2 package that all of downstream reads. The 2026-08 upload layout
# ships the second pair as *_eye_* (older uploads shipped it as *_front_*);
# both entries stay so either layout normalizes, and a pair whose videos are
# absent is skipped like any optional pair.
PAIRS = (
    ("core", "left", "right", "calibration.json"),
    ("eye", "left_eye", "right_eye", "calibration_eye.json"),
    ("front", "left_front", "right_front", "calibration_front.json"),
)

# Independent MONO streams (vendor-confirmed 2026-08-26): left_far and
# right_far are NOT a stereo pair — each camera looks a different way and
# ships its own per-camera Kalibr calibration. Each is published standalone:
# raw stream + own frame-timestamps file + an UNDISTORTED video built from
# its own calibration (fisheye -> pinhole; NOT stereo rectification — the
# two cameras' optical axes diverge ~168 deg, there is no shared view) + a
# calibration file wrapping the vendor doc verbatim plus the undistorted
# view's pinhole intrinsics.
MONO_STREAMS = (
    # (stream stem, per-camera calibration file, aux_streams role)
    ("left_far", "calibration_left_far.json", "far_left"),
    ("right_far", "calibration_right_far.json", "far_right"),
)


def _resolve_pair_calibration(
    pair_name: str, calib_file: str, calibs: dict[str, Path],
) -> "Path | tuple[Path, Path] | None":
    """The pair's calibration input: a stereo file, a per-camera file pair, or None.

    The 2026-08 layout ships the far pair's calibration as TWO per-camera
    Kalibr files (calibration_left_far.json + calibration_right_far.json),
    each carrying that camera's intrinsics plus its T_cam0_imu against the
    SAME IMU — the stereo extrinsic is composed through it by
    load_composed_calibration(). A single stereo file, when present, wins.
    """
    if calib_file in calibs:
        return calibs[calib_file]
    left = calibs.get(f"calibration_left_{pair_name}.json")
    right = calibs.get(f"calibration_right_{pair_name}.json")
    if left is not None and right is not None:
        return (left, right)
    return None


def load_mono_calibration(path: Path) -> dict:
    """Parse one per-camera Kalibr file into {K, D, model, width, height, doc}."""
    doc = json.loads(path.read_text())
    cams = doc.get("cameras") or {}
    if len(cams) != 1:
        raise ValueError(
            f"{path.name}: expected exactly one camera, found {sorted(cams)}")
    cam = next(iter(cams.values()))
    fx, fy, cx, cy = cam["intrinsics"]
    return {
        "K": np.array([[fx, 0, cx], [0, fy, cy], [0, 0, 1.0]], dtype=np.float64),
        "D": np.array(cam["distortion_coefficients"], dtype=np.float64),
        "model": cam.get("distortion_model", "equidistant"),
        "width": int(cam["resolution"][0]),
        "height": int(cam["resolution"][1]),
        "doc": doc,
    }


def undistort_maps_mono(cal: dict) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    """(map1, map2, new_K): fisheye/radtan -> ideal pinhole at balance/alpha 0
    (crop to valid pixels, no black borders) — same posture as the stereo
    pairs' rectification and the mono pipeline's video_rectified."""
    K, D = cal["K"], cal["D"]
    size = (cal["width"], cal["height"])
    if cal["model"] == "equidistant":
        new_K = cv2.fisheye.estimateNewCameraMatrixForUndistortRectify(
            K, D.reshape(-1, 1), size, np.eye(3), balance=0.0)
        m1, m2 = cv2.fisheye.initUndistortRectifyMap(
            K, D.reshape(-1, 1), np.eye(3), new_K, size, cv2.CV_16SC2)
    else:
        new_K, _ = cv2.getOptimalNewCameraMatrix(K, D, size, 0.0, size)
        m1, m2 = cv2.initUndistortRectifyMap(K, D, np.eye(3), new_K, size, cv2.CV_16SC2)
    return m1, m2, new_K


def undistort_stream(
    src: Path, dst: Path, m1: np.ndarray, m2: np.ndarray,
    width: int, height: int, fps: float, expected_frames: int,
    config: NormalizationConfig, source_bitrate_bps: int | None,
    logger: logging.Logger,
) -> None:
    """Decode -> per-frame remap -> encode, one stream. Frame count enforced:
    the output must match `expected_frames` (== the timestamps file's rows) or
    the package's row-i==frame-i contract breaks."""
    cap = cv2.VideoCapture(str(src))
    if not cap.isOpened():
        raise RuntimeError(f"cannot open {src.name}")
    err = open(dst.with_suffix(".encode.log"), "w+")
    proc = subprocess.Popen(
        ["ffmpeg", "-v", "error", "-y",
         "-f", "rawvideo", "-pix_fmt", "bgr24", "-s", f"{width}x{height}",
         "-r", f"{fps:.6f}", "-i", "pipe:0",
         *_encoder_args(config, source_bitrate_bps, 1),
         "-movflags", "+faststart", str(dst)],
        stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=err,
    )
    written = 0
    try:
        while written < expected_frames:
            ok, frame = cap.read()
            if not ok:
                break
            proc.stdin.write(cv2.remap(frame, m1, m2, cv2.INTER_CUBIC).tobytes())
            written += 1
    finally:
        cap.release()
        if proc.stdin:
            proc.stdin.close()
        rc = proc.wait(timeout=600)
        err.seek(0)
        tail = err.read()[-2000:]
        err.close()
        if rc != 0:
            raise RuntimeError(f"encoder failed for {dst.name} (rc={rc}): {tail}")
    if written != expected_frames:
        raise RuntimeError(
            f"{dst.name}: decoded {written} frames, expected {expected_frames} — "
            f"the undistorted stream would break row-i==frame-i")


def load_composed_calibration(
    left_path: Path, right_path: Path, logger: logging.Logger,
) -> AkaiCalibration:
    """Stereo calibration from two per-camera Kalibr files (2026-08 far pair).

    Each file carries ONE camera's intrinsics plus its T_cam0_imu against the
    same IMU, so the inter-camera extrinsic the pair lacks is composed through
    that shared IMU:

        p_camL = T_camL_imu * p_imu    p_camR = T_camR_imu * p_imu
        =>  T_camR_camL = T_camR_imu @ inv(T_camL_imu)

    A composed extrinsic inherits BOTH mono cam-IMU errors (the vendor files
    ship no quality stats for these runs), so the per-chunk rotation-only
    refinement with held-out validation is the quality backstop. The baseline
    LENGTH has no independent check — treat far metric depth accordingly.
    """
    dl = json.loads(left_path.read_text())
    dr = json.loads(right_path.read_text())

    def mono_cam(d: dict, path: Path) -> dict:
        cams = d.get("cameras") or {}
        if len(cams) != 1:
            raise ValueError(
                f"{path.name}: expected exactly one camera in a per-camera "
                f"calibration, found {sorted(cams)}")
        return next(iter(cams.values()))

    cl, cr = mono_cam(dl, left_path), mono_cam(dr, right_path)
    if cl.get("physical_eye") != "left" or cr.get("physical_eye") != "right":
        raise ValueError(
            f"per-camera calibrations are not a left+right pair: "
            f"{left_path.name} physical_eye={cl.get('physical_eye')}, "
            f"{right_path.name} physical_eye={cr.get('physical_eye')}")
    if dl.get("device_id") != dr.get("device_id"):
        raise ValueError(
            f"per-camera calibrations are from different devices: "
            f"{dl.get('device_id')} vs {dr.get('device_id')}")
    imu_l = (dl.get("imu") or {}).get("rostopic")
    imu_r = (dr.get("imu") or {}).get("rostopic")
    if imu_l != imu_r:
        raise ValueError(
            f"per-camera calibrations reference different IMUs ({imu_l} vs "
            f"{imu_r}); the stereo extrinsic can only be composed through ONE")
    if cl.get("resolution") != cr.get("resolution"):
        raise ValueError(
            f"per-camera calibration resolutions differ: "
            f"{cl.get('resolution')} vs {cr.get('resolution')}")

    def K_of(cam: dict) -> np.ndarray:
        fx, fy, cx, cy = cam["intrinsics"]
        return np.array([[fx, 0, cx], [0, fy, cy], [0, 0, 1.0]], dtype=np.float64)

    def D_of(cam: dict) -> np.ndarray:
        return np.array(cam["distortion_coefficients"], dtype=np.float64)

    T_l_imu = np.array(dl["extrinsics"]["T_cam0_imu"], dtype=np.float64)
    T_r_imu = np.array(dr["extrinsics"]["T_cam0_imu"], dtype=np.float64)
    T_rl = T_r_imu @ np.linalg.inv(T_l_imu)   # video-left -> video-right
    R, T = T_rl[:3, :3].copy(), T_rl[:3, 3].reshape(3, 1).copy()
    baseline = float(np.linalg.norm(T))
    rot_deg = float(np.degrees(np.arccos(
        np.clip((np.trace(R) - 1.0) / 2.0, -1.0, 1.0))))
    logger.info(
        f"composed stereo extrinsic through {imu_l}: baseline {baseline * 1000:.2f}mm, "
        f"inter-camera rotation {rot_deg:.2f} deg "
        f"({left_path.name} + {right_path.name})")

    # Kalibr's timeshift key (t_imu = t_camera + shift) — same quantity the
    # stereo files publish as cam_imu_time_offset_s. cam0 (left) defines the
    # pair's timeline downstream, so its value is the pair's offset.
    def shift_of(d: dict) -> float:
        t = d.get("temporal") or {}
        return float(t.get("cam_imu_time_offset_s",
                           t.get("timeshift_cam_imu_s", 0.0)))

    w, h = int(cl["resolution"][0]), int(cl["resolution"][1])
    raw_data = {
        "schema": dl.get("schema"),
        "schema_version": dl.get("schema_version"),
        "device_id": dl.get("device_id"),
        "extrinsics": {
            "matrix_format": "4x4 homogeneous transform",
            "T_cam1_cam0_convention": "p_cam1 = T_cam1_cam0 * p_cam0",
            "T_cam1_cam0": [[float(v) for v in row] for row in T_rl],
            "T_cam0_imu_convention": "p_cam0 = T_cam0_imu * p_imu",
            "T_cam0_imu": [[float(v) for v in row] for row in T_l_imu],
            "T_cam1_imu": [[float(v) for v in row] for row in T_r_imu],
            "stereo_baseline_m": baseline,
        },
        "imu": dl.get("imu") or {},
        "temporal": {
            "cam_imu_time_offset_s": shift_of(dl),
            "timeshift_right_cam_imu_s": shift_of(dr),
        },
        "_provenance": {
            "stereo_extrinsic": "composed through the shared IMU from two "
                                "per-camera Kalibr calibrations; baseline "
                                "length has no independent verification",
            "composed_from": [left_path.name, right_path.name],
            "left": dl.get("_provenance"),
            "right": dr.get("_provenance"),
        },
    }
    return AkaiCalibration(
        width=w, height=h,
        K1=K_of(cl), D1=D_of(cl), K2=K_of(cr), D2=D_of(cr),
        R=R, T=T,
        distortion_model=cl.get("distortion_model", "equidistant"),
        raw_data=raw_data,
    )


# ──────────────────────────────────────────────────────────────────────────────
# IMU salvage
# ──────────────────────────────────────────────────────────────────────────────

def salvage_imu_db(src: Path, logger: logging.Logger) -> tuple[Path | None, dict]:
    """Rebuild an unreadable IMU db with `sqlite3 .recover`.

    All 8 unreadable dbs in the corpus ship a `.db-journal` sibling — a hot
    rollback journal from a recorder that died mid-transaction. Two things
    follow, and only the second one helps:

      * Opening read-WRITE replays the journal, which ROLLS THE TRANSACTION
        BACK and leaves an empty database ("no such table: gyro_data").
        Measured, not assumed. So journal replay is worthless here.
      * `.recover` walks the b-tree pages directly and ignores both the journal
        and the corrupt header. On the one file tested it returned 26,098 gyro
        and 25,944 accel rows, imuid_ 1, 200.90 Hz, ZERO gaps and ZERO
        non-monotonic samples — a clean contiguous time-prefix covering 130 s
        of that segment's 247 s video.

    The recovered stream is a PREFIX, not the whole recording, so the caller
    must clamp the package to it rather than assume full coverage.
    """
    stats = {"attempted": True, "tool": "sqlite3 .recover"}
    if shutil.which("sqlite3") is None:
        stats.update(ok=False, reason="sqlite3 CLI not installed in this image")
        return None, stats
    dst = src.with_suffix(".recovered.db")
    dst.unlink(missing_ok=True)
    try:
        rec = subprocess.run(f'sqlite3 "{src}" .recover', shell=True,
                             capture_output=True, text=True, timeout=600)
        if not rec.stdout.strip():
            stats.update(ok=False, reason="`.recover` produced no output")
            return None, stats
        ins = subprocess.run(f'sqlite3 "{dst}"', shell=True, input=rec.stdout,
                             capture_output=True, text=True, timeout=600)
        if ins.returncode != 0:
            stats.update(ok=False, reason=f"rebuild failed: {ins.stderr[:200]}")
            return None, stats
    except subprocess.TimeoutExpired:
        stats.update(ok=False, reason="salvage timed out")
        return None, stats

    # A salvage is only usable if it still looks like an IMU log. `.recover`
    # will happily emit rows into `lost_and_found` when the schema page itself
    # is gone — those carry no table identity, and guessing which are accel and
    # which are gyro by physics is a coin flip that writes rad/s into an m/s^2
    # column. Refuse that outright.
    try:
        con = sqlite3.connect(dst)
        tabs = {r[0] for r in con.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        if not {"acc_data", "gyro_data"} <= tabs:
            con.close()
            stats.update(ok=False, reason=f"no acc/gyro tables recovered (got {sorted(tabs)})")
            return None, stats
        counts = {t: con.execute(f"SELECT COUNT(*) FROM {t}").fetchone()[0]
                  for t in ("acc_data", "gyro_data")}
        con.close()
    except sqlite3.DatabaseError as exc:
        stats.update(ok=False, reason=f"rebuilt db unreadable: {exc}")
        return None, stats
    if min(counts.values()) < 3:
        stats.update(ok=False, reason=f"too few rows recovered: {counts}")
        return None, stats
    stats.update(ok=True, rows_recovered=counts)
    logger.warning(
        f"IMU {src.name}: unreadable, SALVAGED via .recover -> "
        f"{counts['gyro_data']} gyro / {counts['acc_data']} accel rows. The stream is a "
        f"PREFIX of the recording; the package will be clamped to its span.")
    return dst, stats


def build_grade(core_report: dict, imu_stats: dict, coherence: dict,
                quarantined: list[str], nominal_dt: float) -> dict:
    """Machine-readable delivery grade.

    Two consumers, one package. The customer takes raw+rectified+IMU and does
    not care about VIO preconditions; a head-pose stage must refuse anything it
    cannot solve honestly. Rather than reject the package (which loses the
    customer deliverable) or ship it unmarked (which loses the VIO stage), the
    package always ships and carries its own verdict:

        meta.grade.vio != "full"  ->  a VIO stage refuses, in one check.

    A MISSING grade means "full", so every other device and every pre-existing
    package is unaffected.
    """
    reasons: list[str] = []
    resid = core_report.get("residual_skew_ms")
    cov = core_report.get("coverage")
    gap = core_report.get("longest_gap_s") or 0.0

    if resid is None or resid > PAIR_MATCH_TOLERANCE_S * 1e3:
        reasons.append("STEREO_SKEW_ABOVE_TOLERANCE")
    if cov is not None and cov < COVERAGE_WARN:
        reasons.append(f"COVERAGE_{cov * 100:.0f}PCT")
    if gap > 1.5 * nominal_dt:
        reasons.append("TIMELINE_HAS_INTERIOR_GAP")
    # A chunk whose gyro FRAME was never verified must not claim VIO grade: the
    # gravity fit silently no-ops on short/quiet spans, and it cannot see an
    # axis swap even when it runs.
    if not imu_stats.get("gyro_scale_checked"):
        reasons.append("IMU_GYRO_FRAME_UNVERIFIED")
    if imu_stats.get("salvage"):
        reasons.append("IMU_SALVAGED")
    if imu_stats.get("gap_over_split_threshold"):
        reasons.append("IMU_GAP_OVER_THRESHOLD")
    if core_report.get("discarded_runs"):
        reasons.append("TIMELINE_TRUNCATED_AT_BREAK")
    # Provenance is NOT a VIO blocker. A quarantined AUX stream says nothing
    # about the core pair a head-pose stage actually consumes — the core is
    # coherent or we would have failed the chunk outright. Recording it in
    # `reasons` would refuse a perfectly solvable package.
    provenance: list[str] = []
    if quarantined:
        provenance.append("AUX_STREAM_QUARANTINED")
    if imu_stats.get("salvage"):
        provenance.append("IMU_SALVAGED")

    # Anything at all on the list drops the package out of VIO grade — the
    # customer still gets raw+rectified+IMU, the head-pose stage refuses.
    vio, tier = ("full", "A") if not reasons else ("degraded", "B")
    return {
        "schema": 1,
        "tier": tier,
        "vio": vio,
        "reasons": reasons,              # VIO blockers only
        "provenance_flags": provenance,  # true of the package, not disqualifying
        "core_pair": {"pair": core_report.get("pair", "core"),
                      "substituted_for": None},
        "timing": {
            "stereo_residual_skew_ms": resid,
            "start_skew_ms": core_report.get("start_skew_ms"),
            "modal_index_offset": core_report.get("modal_index_offset"),
            "frame_imu_tie": "measured",
            "timeline_has_gaps": bool(gap > 1.5 * nominal_dt),
            "max_interior_gap_s": gap,
            "discarded_run_s": sum(r.get("frames", 0) * nominal_dt
                                   for r in (core_report.get("discarded_runs") or [])),
            "frames_outside_imu_coverage": core_report.get("frames_outside_imu_coverage"),
            # A VIO stage MUST start preintegrating at this frame, not at 0.
            "imu_covered_frame_range": [core_report.get("imu_uncovered_head_frames") or 0,
                                        (core_report.get("frames_written") or 1) - 1],
            "imu_uncovered_head_frames": core_report.get("imu_uncovered_head_frames"),
        },
        "coverage": cov,
        "imu": {
            "rate_hz": imu_stats.get("rate_hz"),
            "gyro_scale_checked": imu_stats.get("gyro_scale_checked"),
            "salvaged": bool(imu_stats.get("salvage")),
            "gaps": len(imu_stats.get("gaps") or []),
        },
        "segment_coherence_spread_s": coherence.get("spread_s"),
        "quarantined_streams": quarantined,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Chunk discovery
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class BitrobotChunk:
    """One RoboCap chunk: 6 videos, 2 IMUs, a magnetometer, 2 calibrations."""

    chunk_num: str                     # "000"
    videos: dict[str, Path]            # stem -> mp4, e.g. "left_far" -> path
    imu_calibrated: Path               # imu_right_XXX.db (imuid_=1)
    imu_secondary: Path | None         # imu_XXX.db (imuid_=2), not consumed
    mag: Path | None                   # mag_middle_XXX.db, not consumed
    calibrations: dict[str, Path]      # filename -> path


def find_chunk(session_dir: Path, logger: logging.Logger) -> tuple[BitrobotChunk | None, list[str]]:
    """Locate the single chunk in `session_dir` and report what is missing."""
    errors: list[str] = []

    calib = session_dir / "calibration.json"
    if not calib.exists():
        return None, ["calibration.json not found"]

    # The chunk number is derived from ANY of the six camera stems, not from
    # left_XXX.mp4 alone: 45 streams in the corpus are absent outright and 4 are
    # zero-byte, and when the missing one happened to be `left` the whole chunk
    # was unfindable even though five cameras and both IMUs were present.
    nums: set[str] = set()
    for _, lstem, rstem, _ in PAIRS:
        for stem in (lstem, rstem):
            for p in session_dir.glob(f"{stem}_[0-9][0-9][0-9].mp4"):
                m = re.match(rf"{stem}_(\d+)\.mp4$", p.name)
                if m:
                    nums.add(m.group(1))
    if not nums:
        return None, ["no <stem>_XXX.mp4 found for any of the six cameras"]
    if len(nums) > 1:
        errors.append(
            f"expected one chunk per directory, found {len(nums)}: "
            + ", ".join(sorted(nums)))
    num = sorted(nums)[0]

    videos: dict[str, Path] = {}
    stems = [(stem, pair_name == "core")
             for pair_name, lstem, rstem, _ in PAIRS for stem in (lstem, rstem)]
    stems += [(m[0], False) for m in MONO_STREAMS]
    for stem, required in stems:
        p = session_dir / f"{stem}_{num}.mp4"
        if p.exists():
            videos[stem] = p
        elif required:
            # Aux pairs/streams are optional per layout (eye vs front naming);
            # only the core pair's absence is worth an error.
            errors.append(f"missing video: {p.name}")

    # IMU files, both upload layouts. The imuid_ guard in load_imu verifies
    # the pick either way (calibrated sensor = imuid_ 1, secondary = 2).
    #   pre-2026-08: imu_right_XXX.db = calibrated, imu_XXX.db = secondary
    #   2026-08:     imu_XXX.db = calibrated, imu_left_XXX.db = secondary
    imu_cal = session_dir / f"imu_right_{num}.db"
    secondary = session_dir / f"imu_{num}.db"
    if not imu_cal.exists():
        imu_cal = session_dir / f"imu_{num}.db"
        secondary = session_dir / f"imu_left_{num}.db"
    if not imu_cal.exists():
        errors.append(
            f"missing calibrated IMU: imu_right_{num}.db / imu_{num}.db")
        return None, errors

    mag = session_dir / f"mag_middle_{num}.db"
    calibs = {p.name: p for p in session_dir.glob("calibration*.json")}

    logger.info(
        f"chunk {num}: {len(videos)}/6 videos, calibrations={sorted(calibs)}, "
        f"secondary_imu={'yes' if secondary.exists() else 'no'}"
    )
    return (
        BitrobotChunk(
            chunk_num=num,
            videos=videos,
            imu_calibrated=imu_cal,
            imu_secondary=secondary if secondary.exists() else None,
            mag=mag if mag.exists() else None,
            calibrations=calibs,
        ),
        errors,
    )


def read_raw_sensor_scales(calib_path: Path, logger: logging.Logger) -> tuple[float, float]:
    """Counts->SI divisors for THIS unit, from calibration.json.

    The device ships them in _provenance.raw_sensor_scales, which is
    authoritative per unit. They are deliberately NOT taken from
    NormalizationConfig.imu_*: those fields carry akai's ICM-20948 values
    (16818 counts/g), and silently applying them to RoboCap's ICM-42688-P
    (8192 LSB/g) scales every acceleration by 0.49 -- an imu.csv that parses,
    validates, and is wrong by a factor of two.
    """
    try:
        prov = json.loads(calib_path.read_text()).get("_provenance", {})
        scales = prov.get("raw_sensor_scales", {})
        accel = float(scales["accel_lsb_per_g"])
        gyro = float(scales["gyro_lsb_per_dps"])
        logger.info(f"IMU scales from {calib_path.name}: {accel} LSB/g, {gyro} LSB/dps")
        return accel, gyro
    except (OSError, ValueError, KeyError, TypeError) as exc:
        # Deliberately NOT a warn-and-default. Falling back silently is how a
        # future unit with different full-scale settings would be converted with
        # the wrong divisors and still produce an imu.csv that parses, validates
        # and looks plausible -- the exact failure this module exists to prevent.
        raise ValueError(
            f"{calib_path.name}: _provenance.raw_sensor_scales is missing or unreadable "
            f"({exc}). Counts->SI divisors are per-unit and must not be guessed; refusing "
            f"to fall back to {DEFAULT_ACCEL_LSB_PER_G} LSB/g / {DEFAULT_GYRO_LSB_PER_DPS} LSB/dps."
        ) from exc


# ──────────────────────────────────────────────────────────────────────────────
# IMU
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class BitrobotImu:
    t_device_s: np.ndarray   # accelerometer timestamps, device clock, seconds
    accel: np.ndarray        # (N,3) m/s^2
    gyro: np.ndarray         # (N,3) rad/s, interpolated onto t_device_s
    metadata: dict
    stats: dict


# Acceptable fitted gyro scale factor. 1.0 is correct; the neighbouring
# full-scale settings are a clean 2x or 0.5x away, so this band accepts real
# calibration slop while rejecting a wrong divisor.
GYRO_SCALE_SANITY_RATIO = (0.70, 1.45)


def _gyro_scale_factor(
    t: np.ndarray, accel: np.ndarray, gyro: np.ndarray, window_s: float = 0.5
) -> float | None:
    """Estimate the multiplicative error in the gyro scale, from physics alone.

    Over a short window the rotation the gyro reports must carry the measured
    gravity direction at the start onto the gravity direction at the end. So we
    scan a multiplier k, rotate g0 by the gyro-integrated rotation scaled by k,
    and keep the k that best reproduces g1 across all usable windows. k ~= 1
    means the counts->rad/s divisor is right; k ~= 2 means it is off by 2x.

    This is deliberately a FITTED factor rather than a ratio-of-angles compared
    against a fixed band. The plain angle ratio has a motion-dependent baseline
    (the gyro integrates the whole path, the accelerometer sees only the net
    chord, and yaw about gravity is invisible to it), so its expected value
    drifts with the recording and cannot cleanly separate 1x from 2x. The
    argmin does not care about that baseline -- whatever systematic offset the
    motion introduces applies equally at every k.

    Returns None when the recording has too few usable windows to judge.
    """
    n = int(window_s / max(np.median(np.diff(t)), 1e-9))
    if n < 4 or len(t) < 4 * n:
        return None

    pairs: list[tuple[np.ndarray, np.ndarray, np.ndarray]] = []
    # Stride at n/4, not n. At full-window stride a 31 s chunk yields ~13
    # usable windows against a 20-window floor, so the check returned None and
    # the caller skipped it SILENTLY -- 5% of this corpus is <=21 s. Overlapping
    # windows are correlated, but the fit is a median over windows and the
    # argmin is unaffected by that correlation.
    for i in range(0, len(t) - n, max(n // 4, 1)):
        a0, a1 = accel[i], accel[i + n]
        # Trust the endpoints only when the device is near 1 g at both, i.e.
        # gravity dominates and linear acceleration is small.
        if not (0.9 < np.linalg.norm(a0) / G < 1.1):
            continue
        if not (0.9 < np.linalg.norm(a1) / G < 1.1):
            continue
        g0 = a0 / np.linalg.norm(a0)
        g1 = a1 / np.linalg.norm(a1)
        if np.degrees(np.arccos(np.clip(np.dot(g0, g1), -1.0, 1.0))) < 6.0:
            continue  # too little rotation to be informative
        # Chain the rotation incrementally instead of integrating the window to
        # one axis-angle vector. np.trapezoid + a single Rodrigues treats 0.5 s
        # of motion as a FIXED-AXIS rotation, and SO(3) does not commute: at the
        # 20-80 dps seen here that inflates the fitted k (measured 1.15-1.25 on
        # known-good data, and denser striding pushed a real chunk to 1.40 —
        # past the old 1.35 ceiling — rejecting good IMU data).
        seg = gyro[i:i + n + 1]
        ts = t[i:i + n + 1]
        pairs.append((seg, np.diff(ts), g0, g1))
    if len(pairs) < 20:
        return None

    def residual(k: float) -> float:
        errs = []
        for seg, dts, g0, g1 in pairs:
            R = np.eye(3)
            # gravity in the body frame rotates by -w when the body rotates by +w
            for w, dt in zip(seg[:-1], dts):
                R = R @ cv2.Rodrigues(-k * w * dt)[0]
            errs.append(float(np.linalg.norm(R @ g0 - g1)))
        return float(np.median(errs))

    ks = np.concatenate([np.linspace(0.2, 3.0, 57), [1.0]])
    return float(ks[int(np.argmin([residual(k) for k in ks]))])


# The calibrated IMU is Kalibr's imu0 == recorder imuid_ 1 == imu_right_*.db.
CALIBRATED_IMUID = 1

# Sample rate is a per-unit CRYSTAL CONSTANT, not a health signal, and it is a
# useless wrong-sensor detector. Measured over 3,001 imu_right_*.db across 20
# devices: 197.56-203.05 Hz, while imu_left (the WRONG sensor) spans
# 197.27-202.38 Hz -- the two populations overlap almost completely, and 2,150
# of 3,001 correct files sit below the wrong sensor's maximum. The old +/-0.5%
# gate against the calibration's declared 202.3 Hz therefore rejected 34.5%
# (1034/3001) of perfectly good chunks while still admitting ~46% of
# wrong-sensor files. It was comparing one unit's Kalibr-observed ODR against a
# different unit's crystal.
#
# What actually detects the failure the rate check was gesturing at:
#   - imuid_ == CALIBRATED_IMUID          (exact wrong-sensor test)
#   - deviceid(db) == deviceid(videos)    (exact wrong-calibration/mixed-chunk test)
# Rate is demoted to a plausibility assertion: "this is a ~200 Hz IMU log at
# all", which catches a truncated db or a spliced/wrong-table file. Costs 0 of
# 3,001 real files.
IMU_RATE_PLAUSIBILITY = 0.10   # fatal outside +/-10% of declared
IMU_RATE_NOTICE = 0.005        # informational warning outside +/-0.5%
# acc and gyro come off the same part; their measured rates agree to p99 0.38%.
IMU_ACC_GYRO_RATE_TOLERANCE = 0.01

# ── Timeline defects (all thresholds derived from the 4.27 TB QC sweep) ───────
#
# STEREO ASSOCIATION. The two eyes have no shared frame trigger, so the pairing
# must be made on timestamps (docs/06 invariant 1: "match frames by timestamp,
# drop unmatched edges"). Measured |dt| between an eye pair's capture starts is
# strictly bimodal: 7,078 pairs land within 1 ms (typically 0.03 ms) and 1,771
# land at exactly one frame period (33.15-33.53 ms). NOTHING lands between 1 ms
# and 20 ms. So the one-frame cluster is an INDEXING offset from a capture-start
# race, not an exposure offset -- re-association removes it entirely, and the
# residual on healthy data is <=0.172 ms.
PAIR_MATCH_WINDOW_FRAMES = 0.5   # nearest-neighbour search bound, in frame periods
PAIR_MATCH_TOLERANCE_S = 0.001   # a candidate worse than this is NOT a stereo pair
#   1 ms is 5.8x above the worst residual healthy data achieves, sits inside
#   OKVIS2's own configured timestamp_tolerance (5 ms, okvis/build_clip.py), and
#   at the rectified focal length (672 px for 110 deg HFOV) costs 1.4 px at a
#   120 deg/s peak rate. Half a frame period -- the usual greedy bound -- would
#   accept every mispaired frame in this corpus.

# SEGMENT COHERENCE. 44 segments hold two recordings ~3.5 h apart uploaded under
# identical robocap_segment<N>_* names, so the six streams are not one capture.
# Across 2,975 readable segments the largest LEGITIMATE spread between adjacent
# camera starts is 0.0334 s (one frame, the startup race) and the smallest
# COLLISION is 1.4332 s -- the band between is completely empty. 10 frame
# periods sits 10x above the largest good value and 4.3x below the nearest bad
# one, and rescales if a future unit runs at 60 fps.
SEGMENT_COHERENCE_FRAMES = 10.0
SEGMENT_COHERENCE_WARN_FRAMES = 2.0

# CONTINUITY. A gap longer than this is not one trajectory: at ~1.4 m/s, 2 s is
# ~2.8 m of translation with no visual overlap in a 110 deg FOV, every feature
# track is gone, and IMU-only propagation across it (with the ~0.5 dps bias this
# adapter deliberately leaves in-state) is already ~4 cm / ~1 deg off -- outside
# a keyframe-matching basin. Represent it as two runs, never one package
# spanning the hole.
VIDEO_GAP_SPLIT_S = 2.0
MIN_RUN_S = 20.0            # a shorter surviving run is not worth a package
MIN_PAIR_COVERAGE = 0.50    # below this the pair is rejected outright
COVERAGE_WARN = 0.98

# IMU CONTINUITY. Measured: normal jitter tops out at max_dt/median_dt = 1.10
# (p99), and the smallest genuine dropout is 69.2 ms -- so any threshold in
# [10 ms, 60 ms] partitions this corpus identically. 50 ms is the defensible
# one: chained SO(3) integration across a 50 ms hole costs p95 1.05 deg (~18 px
# at 17.45 px/deg), a 160-sigma error on a preintegration factor that carries no
# robust kernel. Unmodelled angular motion dominates gyro bias by ~30x here, so
# this is sized off motion, not off ICM-42688-P noise.
IMU_GAP_SPLIT_S = 0.050
# Never interpolate a gyro sample whose bracketing pair straddles a real hole.
IMU_INTERP_MAX_BRACKET_FACTOR = 3.0
MIN_RETAINED_SPAN_S = 5.0

# When frames have to be CUT from the head or interior, the raw pair can no
# longer be stream-copied (30-frame GOP, and edit lists are not honoured
# consistently by cv2/ffmpeg readers). It is then re-encoded from the decoded
# frames -- but at a visually-lossless quality, NOT at the rectified trio's
# rate. The rectified pair is a remap of the source and its CRF is a storage
# choice; left_raw/right_raw are what okvis/build_clip.py actually feeds to the
# VIO front end, so one generation there has to be invisible to a feature
# detector. Only ever applied when the alternative is a mistimed stereo pair.
RAW_REENCODE_CRF = 14


def load_imu(
    db_path: Path,
    logger: logging.Logger,
    accel_lsb_per_g: float = DEFAULT_ACCEL_LSB_PER_G,
    gyro_lsb_per_dps: float = DEFAULT_GYRO_LSB_PER_DPS,
    expected_rate_hz: float | None = None,
    expected_imuid: int = CALIBRATED_IMUID,
    allow_salvage: bool = True,
) -> BitrobotImu:
    """Read imu_right_XXX.db into SI units on ONE timestamp grid.

    acc_data and gyro_data are logged from independently drained sensor FIFOs,
    so they never share a timestamp and must be resampled. The accelerometer
    grid is the master and gyro is linearly interpolated onto it, restricted to
    the overlap -- interpolating inside the overlap is honest, extrapolating
    past either end is not.

    Raises on the failure modes that must never be papered over with zeros:
    missing/empty tables, non-monotonic clocks, no acc/gyro overlap.
    """
    accel_to_ms2 = G / accel_lsb_per_g
    gyro_to_rads = (1.0 / gyro_lsb_per_dps) * (math.pi / 180.0)

    if not db_path.exists():
        raise StreamUnreadable("ABSENT", db_path)
    if db_path.stat().st_size == 0:
        raise StreamUnreadable("ZERO_BYTE", db_path)
    conn = None
    try:
        conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
        meta = dict(conn.execute("SELECT key, value FROM metadata").fetchall())
        rows = {}
        for table in ("acc_data", "gyro_data"):
            data = conn.execute(
                f"SELECT x, y, z, timestamp FROM {table} ORDER BY timestamp"
            ).fetchall()
            if not data:
                raise ValueError(f"{db_path.name}/{table}: zero rows")
            rows[table] = np.asarray(data, dtype=np.float64)
        ids = {
            int(r[0])
            for r in conn.execute(
                "SELECT DISTINCT imuid_ FROM acc_data "
                "UNION SELECT DISTINCT imuid_ FROM gyro_data"
            )
        }
    except sqlite3.DatabaseError as exc:
        # v1 gave up here. v2 tries `.recover` first — all 8 unreadable dbs in
        # the corpus are recoverable to a clean (if truncated) timeline.
        if conn is not None:
            try: conn.close()
            except Exception: pass
            conn = None
        if not allow_salvage:
            raise StreamUnreadable("CORRUPT_DB", db_path, str(exc))
        rescued, sstats = salvage_imu_db(db_path, logger)
        if rescued is None:
            raise StreamUnreadable(
                "CORRUPT_DB", db_path, f"{exc}; salvage failed: {sstats.get('reason')}")
        out = load_imu(rescued, logger, accel_lsb_per_g=accel_lsb_per_g,
                       gyro_lsb_per_dps=gyro_lsb_per_dps,
                       expected_rate_hz=expected_rate_hz,
                       expected_imuid=expected_imuid, allow_salvage=False)
        out.stats["salvage"] = {**sstats, "original_error": str(exc)[:200],
                                "source_db": db_path.name}
        return out
    finally:
        if conn is not None:
            conn.close()

    if len(ids) != 1:
        raise ValueError(f"{db_path.name}: expected one imuid_, found {sorted(ids)}")
    # Checking the count alone is not a guard: imu_XXX.db also holds exactly one
    # imuid_ (2) and would sail through, producing a superficially plausible
    # imu.csv (its median |accel| is 9.773 vs 9.774) built from a sensor the
    # calibration says is ~179 deg rotated and ~117 mm away from the one
    # T_cam0_imu describes.
    if sorted(ids)[0] != expected_imuid:
        raise ValueError(
            f"{db_path.name}: imuid_={sorted(ids)[0]}, expected {expected_imuid}. "
            f"calibration.json describes imu0 == /imu_right_1 == imuid_ 1; any other "
            f"sensor is uncalibrated and would silently corrupt every VIO solve."
        )

    acc, gyr = rows["acc_data"], rows["gyro_data"]
    t_a, t_g = acc[:, 3] / 1e9, gyr[:, 3] / 1e9  # timestamps are NANOSECONDS
    for name, t in (("acc_data", t_a), ("gyro_data", t_g)):
        if np.any(np.diff(t) <= 0):
            raise ValueError(f"{db_path.name}/{name}: timestamps not strictly increasing")

    lo, hi = max(t_a[0], t_g[0]), min(t_a[-1], t_g[-1])
    if not hi > lo:
        raise ValueError(f"{db_path.name}: acc and gyro timestamps do not overlap")
    keep = (t_a >= lo) & (t_a <= hi)

    # Restricting to the acc/gyro overlap is LOAD-BEARING, not tidiness:
    # np.interp clamp-EXTRAPOLATES outside its x-range, so without this mask a
    # head/tail overhang would be filled with a held constant presented as
    # measurement.
    #
    # Inside the overlap, linear interpolation is free: each accel timestamp
    # sits a median 0.21 ms from the nearest gyro sample (the ~5 ms t0 offset is
    # a one-INDEX FIFO offset, not a phase offset), and linear-vs-spline differs
    # by a median 0.012 dps -- below the 0.0153 dps quantization step.
    #
    # But interpolating ACROSS a real dropout invents motion. Drop any output
    # row whose bracketing gyro pair straddles a hole: chained SO(3) integration
    # across even 50 ms of unobserved motion costs p95 ~1 deg, and OKVIS2
    # preintegrates it as a single trapezoidal step with a covariance that grows
    # only linearly in dt -- a wrong mean carrying a confident sigma.
    dt_g = np.diff(t_g)
    g_nominal = float(np.median(dt_g)) if dt_g.size else 0.0
    bracket = np.clip(np.searchsorted(t_g, t_a[keep]) - 1, 0, max(len(t_g) - 2, 0))
    straddles = (dt_g[bracket] > IMU_INTERP_MAX_BRACKET_FACTOR * g_nominal
                 if dt_g.size else np.zeros(int(keep.sum()), bool))
    n_straddle = int(straddles.sum())

    idx = np.flatnonzero(keep)[~straddles]
    t_master = t_a[idx]
    accel_ms2 = acc[idx, :3] * accel_to_ms2
    gyro_rads = np.column_stack(
        [np.interp(t_master, t_g, gyr[:, i] * gyro_to_rads) for i in range(3)]
    )
    if t_master.size < 3:
        raise ValueError(
            f"{db_path.name}: only {t_master.size} samples survive the acc/gyro "
            f"overlap and dropout masks")

    dt_a = np.diff(t_a)
    amag = np.linalg.norm(accel_ms2, axis=1)
    stats = {
        "source_db": db_path.name,
        "imuid": sorted(ids)[0],
        "n_accel_raw": int(len(acc)),
        "n_gyro_raw": int(len(gyr)),
        "n_output": int(len(t_master)),
        "n_trimmed_to_overlap": int((~keep).sum()),
        # RAW head/tail, before the gyro-overlap trim. The rebase origin must key
        # off raw_t_start_s, never t_master[0]: the number of samples trimmed at
        # the head varies per chunk (3 in chunk 000, 2 in chunk 001), so an origin
        # derived from the post-trim grid is not reproducible by any other writer
        # and shifts chunk to chunk.
        # min(acc, gyro): in 11.2% of files the GYRO starts first, and this is
        # the value the shared rebase origin keys off, so taking t_a[0] alone
        # would make the origin depend on which sensor happened to lead.
        "raw_t_start_s": float(min(t_a[0], t_g[0])),
        "raw_t_end_s": float(max(t_a[-1], t_g[-1])),
        "device_t_start_s": float(t_master[0]),
        "device_t_end_s": float(t_master[-1]),
        "head_trim_s": float(t_master[0] - t_a[0]),
        "duration_s": float(t_master[-1] - t_master[0]),
        "rate_hz": float(1.0 / np.median(dt_a)),
        "dt_jitter_std_ms": float(dt_a.std() * 1e3),
        "dropout_gaps": int((dt_a > DROPOUT_GAP_FACTOR * np.median(dt_a)).sum()),
        "accel_saturated_samples": int((np.abs(acc[:, :3]) >= SATURATION_COUNT).any(1).sum()),
        "gyro_saturated_samples": int((np.abs(gyr[:, :3]) >= SATURATION_COUNT).any(1).sum()),
        "accel_magnitude_median_ms2": float(np.median(amag)),
        "gyro_median_rads": [float(v) for v in np.median(gyro_rads, axis=0)],
        "accel_lsb_per_g": accel_lsb_per_g,
        "gyro_lsb_per_dps": gyro_lsb_per_dps,
    }

    # Gap accounting, wired to something instead of computed and discarded.
    gap_mask = dt_a > DROPOUT_GAP_FACTOR * np.median(dt_a)
    gaps = [{"at_s": float(t_a[i] - t_a[0]),
             "gap_ms": float(dt_a[i] * 1e3),
             "n_missing": int(round(dt_a[i] / np.median(dt_a))) - 1}
            for i in np.flatnonzero(gap_mask)]
    stats["gaps"] = gaps
    stats["gyro_interp_rows_suppressed"] = n_straddle
    stats["longest_gap_s"] = float(dt_a.max()) if dt_a.size else 0.0
    stats["gap_over_split_threshold"] = [g for g in gaps
                                         if g["gap_ms"] > IMU_GAP_SPLIT_S * 1e3]
    # Saturation rails, published so no consumer has to guess them. (okvis's
    # build_clip.py falls back to (20.0 m/s^2, 18.0 rad/s) for unknown devices,
    # which is BELOW this part's accel rail and ABOVE its gyro rail -- it
    # de-weights ordinary footfalls and never notices a real gyro clip.)
    stats["accel_saturation_ms2"] = float(32767.0 / accel_lsb_per_g * G)
    stats["gyro_saturation_rads"] = float(
        32767.0 / gyro_lsb_per_dps * math.pi / 180.0)

    if expected_rate_hz:
        stats["declared_rate_hz"] = float(expected_rate_hz)
        drift = abs(stats["rate_hz"] - expected_rate_hz) / expected_rate_hz
        stats["rate_drift"] = float(drift)
        # Plausibility only -- see IMU_RATE_PLAUSIBILITY. The wrong-sensor and
        # wrong-calibration failures are caught exactly by imuid_ and deviceid.
        if drift > IMU_RATE_PLAUSIBILITY:
            raise ValueError(
                f"{db_path.name}: measured {stats['rate_hz']:.2f} Hz against a declared "
                f"{expected_rate_hz:.2f} Hz ({drift * 100:.1f}% off). Outside +/-"
                f"{IMU_RATE_PLAUSIBILITY * 100:.0f}% this is not a ~{expected_rate_hz:.0f} Hz "
                f"IMU log at all -- truncated db, wrong table, or spliced file."
            )
        if drift > IMU_RATE_NOTICE:
            logger.info(
                f"IMU {db_path.name}: {stats['rate_hz']:.2f} Hz vs declared "
                f"{expected_rate_hz:.2f} Hz ({drift * 100:.2f}% off). Expected -- rate is a "
                f"per-unit crystal constant (corpus spans 197.6-203.1 Hz) and every sample "
                f"carries its own timestamp, so this is informational."
            )
    # acc and gyro come off the SAME part; disagreement means a spliced or
    # mismatched pair of tables. Measured p99 across the corpus is 0.38%.
    rate_g = 1.0 / float(np.median(dt_g)) if dt_g.size else 0.0
    stats["rate_hz_gyro"] = rate_g
    if rate_g and abs(rate_g - stats["rate_hz"]) / stats["rate_hz"] > IMU_ACC_GYRO_RATE_TOLERANCE:
        raise ValueError(
            f"{db_path.name}: acc {stats['rate_hz']:.2f} Hz vs gyro {rate_g:.2f} Hz differ by "
            f"more than {IMU_ACC_GYRO_RATE_TOLERANCE * 100:.0f}% -- these two tables did not "
            f"come from the same sensor run."
        )

    logger.info(
        f"IMU {db_path.name}: imuid={stats['imuid']} {stats['n_output']} samples "
        f"@ {stats['rate_hz']:.2f} Hz over {stats['duration_s']:.1f} s, "
        f"median |accel| {stats['accel_magnitude_median_ms2']:.2f} m/s^2 (expect ~9.81)"
    )
    # The accel scale is guarded by gravity below, but a wrong GYRO divisor
    # (65.5 vs akai's 32.8) doubles every angular rate and leaves an imu.csv
    # that parses, validates and looks entirely plausible. Cross-check it
    # against physics: over short windows the angle the gyro says the device
    # turned through must match the angle the gravity vector actually swung
    # through. Ratio ~1 = right scale; ~2 or ~0.5 = wrong full-scale setting.
    k = _gyro_scale_factor(t_master, accel_ms2, gyro_rads)
    if k is None:
        # Never skip silently: this is the only check that can catch a wrong
        # gyro full-scale setting or a permuted/flipped gyro frame, both of
        # which produce an imu.csv that parses, validates and looks plausible.
        logger.warning(
            f"IMU {db_path.name}: gyro-scale cross-check SKIPPED -- too few windows with "
            f"enough gravity rotation ({stats['duration_s']:.1f}s span). The counts->SI gyro "
            f"divisor is unverified for this chunk.")
        stats["gyro_scale_checked"] = False
    else:
        stats["gyro_scale_checked"] = True
    if k is not None:
        stats["gyro_scale_factor_fitted"] = k
        lo_g, hi_g = GYRO_SCALE_SANITY_RATIO
        stats["gyro_scale_in_band"] = bool(lo_g <= k <= hi_g)
        if not stats["gyro_scale_in_band"]:
            # REPORT-ONLY (2026-08-26): this used to raise and fail the chunk.
            # Real chunks with a correct divisor fit as high as 1.55 (the
            # estimator runs hot on unfavorable motion — see the striding note
            # above, which already forced the ceiling from 1.35 to 1.45), while
            # a truly wrong full-scale setting fits at a clean ~2x or ~0.5x.
            # The fitted k is recorded in stats/meta for audit; nothing gates
            # on it and downstream behavior is unchanged.
            logger.warning(
                f"IMU {db_path.name}: gyro scale fitted against the accelerometer's "
                f"gravity rotation is {k:.2f}x (expected ~1.0, band [{lo_g}, {hi_g}]). "
                f"If this were a wrong full-scale setting it would fit at ~2x or "
                f"~0.5x of the declared {gyro_lsb_per_dps} LSB/dps divisor. "
                f"Report-only: the chunk proceeds unchanged."
            )

    lo, hi = ACCEL_MAGNITUDE_SANITY_MS2
    if not lo <= stats["accel_magnitude_median_ms2"] <= hi:
        raise ValueError(
            f"{db_path.name}: median |accel| {stats['accel_magnitude_median_ms2']:.3f} m/s^2 is "
            f"outside [{lo}, {hi}] -- expected ~{G:.2f} (1 g). The counts->SI scale is wrong "
            f"(using {accel_lsb_per_g} LSB/g) or the IMU is dead. Refusing to write imu.csv."
        )
    if stats["accel_saturated_samples"]:
        logger.warning(
            f"IMU: {stats['accel_saturated_samples']} accelerometer sample(s) clipped at "
            f"+/-4 g full scale"
        )
    # Gyro clipping was counted but never surfaced -- backwards, since a clipped
    # gyro destroys preintegrated ROTATION outright while a clipped accel is
    # largely absorbed by the bias state.
    if stats["gyro_saturated_samples"]:
        logger.warning(
            f"IMU: {stats['gyro_saturated_samples']} gyroscope sample(s) clipped at "
            f"+/-500 dps full scale -- preintegrated rotation across those samples is "
            f"wrong, not merely noisy"
        )
    if stats["gaps"]:
        logger.warning(
            f"IMU {db_path.name}: {len(stats['gaps'])} dropout(s), longest "
            f"{stats['longest_gap_s'] * 1e3:.1f} ms, {n_straddle} interpolated gyro row(s) "
            f"suppressed so no motion is invented across a hole"
        )

    return BitrobotImu(t_master, accel_ms2, gyro_rads, meta, stats)


def imu_samples(imu: BitrobotImu, t_origin_s: float) -> list[ImuSample]:
    """Rebase onto the shared origin and hand back schema ImuSamples."""
    t = imu.t_device_s - t_origin_s
    return [
        ImuSample(
            timestamp_seconds=float(ts),
            accel_x=float(a[0]), accel_y=float(a[1]), accel_z=float(a[2]),
            gyro_x=float(g[0]), gyro_y=float(g[1]), gyro_z=float(g[2]),
        )
        for ts, a, g in zip(t, imu.accel, imu.gyro)
    ]


# ──────────────────────────────────────────────────────────────────────────────
# Video probing / frame timing
# ──────────────────────────────────────────────────────────────────────────────

class StreamUnreadable(Exception):
    """A source stream cannot be used, with a machine-readable reason.

    Raised instead of letting CalledProcessError/KeyError/ValueError escape
    untyped, so callers can apply the degradation ladder (core pair -> fail the
    chunk; aux pair -> skip that pair) and so chunk_report.json records WHY.

    Codes:
      ABSENT           the file is not there at all
      ZERO_BYTE        zero-length object (4 exist in the corpus)
      NO_MOOV          mdat size 0 and no moov -- the recorder was killed before
                       finalize. 147 exist. The payload is probably intact but
                       the `comment` tag lives in moov/udta and died with it, so
                       the frame<->IMU tie is unrecoverable here; salvage is an
                       offline tool's job, not normalization's.
      NO_COMMENT_TAG   finalized but no device-clock start time
      NO_FRAME_COUNT   ffprobe cannot report nb_frames/duration
      UNREADABLE       anything else
    """

    def __init__(self, code: str, path: Path, detail: str = ""):
        self.code = code
        self.path = path
        self.detail = detail
        super().__init__(f"{path.name}: {code}" + (f" ({detail})" if detail else ""))


def classify_unreadable(path: Path) -> str | None:
    """Cheap structural triage before ffprobe, so the reason is precise."""
    if not path.exists():
        return "ABSENT"
    if path.stat().st_size == 0:
        return "ZERO_BYTE"
    # Walk top-level boxes: a finalized mp4 has a moov; an interrupted one has
    # `mdat` with size 0 ("extends to EOF") and nothing after it.
    try:
        with path.open("rb") as f:
            off, size = 0, path.stat().st_size
            while off + 8 <= size:
                f.seek(off)
                hdr = f.read(16)
                if len(hdr) < 8:
                    break
                box = struct.unpack(">I", hdr[0:4])[0]
                typ = hdr[4:8]
                adv = 8
                if box == 1:
                    box = struct.unpack(">Q", hdr[8:16])[0]
                    adv = 16
                if typ == b"moov":
                    return None
                if box == 0:
                    return "NO_MOOV"      # placeholder never patched
                if box < adv:
                    return "UNREADABLE"
                off += box
    except OSError:
        return "UNREADABLE"
    return "NO_MOOV"


def _ffprobe_json(path: Path, args: list[str]) -> dict:
    try:
        out = subprocess.run(
            ["ffprobe", "-v", "error", *args, "-of", "json", str(path)],
            capture_output=True, text=True, check=True,
        ).stdout
        return json.loads(out)
    except subprocess.CalledProcessError as exc:
        raise StreamUnreadable("UNREADABLE", path, (exc.stderr or "").strip()[:200])
    except json.JSONDecodeError as exc:
        raise StreamUnreadable("UNREADABLE", path, f"ffprobe emitted no JSON: {exc}")


@dataclass
class VideoInfo:
    path: Path
    width: int
    height: int
    codec: str
    frame_count: int
    duration_s: float
    fps: float
    start_device_s: float   # from the container `comment` tag (microseconds)
    bitrate_bps: int | None  # source rate; derived outputs are capped against it
    has_b_frames: int = 0   # non-zero => decode order != presentation order


def probe_video(path: Path) -> VideoInfo:
    """Stream geometry plus the device-clock start time from the comment tag.

    Every failure path raises StreamUnreadable with a code, never a bare
    CalledProcessError/KeyError/ValueError: 196 of the 17,979 source files in
    this corpus are absent, zero-byte or unfinalized, and an untyped raise here
    took the whole chunk down even when the defect was in an aux stream.
    """
    code = classify_unreadable(path)
    if code:
        raise StreamUnreadable(code, path)
    d = _ffprobe_json(
        path,
        ["-select_streams", "v:0", "-show_entries",
         "stream=width,height,codec_name,nb_frames,duration,bit_rate,has_b_frames"
         ":format_tags=comment,deviceid"],
    )
    if not d.get("streams"):
        raise StreamUnreadable("UNREADABLE", path, "no video stream")
    s = d["streams"][0]
    tags = d.get("format", {}).get("tags", {}) or {}
    comment = tags.get("comment")
    if comment is None or not str(comment).strip().isdigit():
        raise StreamUnreadable(
            "NO_COMMENT_TAG", path,
            f"tag={comment!r}; it is the video's start time on the device clock and "
            "the only thing tying frames to the IMU -- refusing to guess it")
    try:
        n = int(s["nb_frames"])
        dur = float(s["duration"])
    except (KeyError, ValueError) as exc:
        raise StreamUnreadable("NO_FRAME_COUNT", path, str(exc))
    return VideoInfo(
        path=path,
        width=int(s["width"]),
        height=int(s["height"]),
        codec=s["codec_name"],
        frame_count=n,
        duration_s=dur,
        fps=(n - 1) / dur if dur > 0 and n > 1 else 0.0,
        start_device_s=int(comment) / 1e6,   # tag is MICROSECONDS
        bitrate_bps=int(s["bit_rate"]) if str(s.get("bit_rate", "")).isdigit() else None,
        # `-frames:v n -c copy` truncates in DECODE order while frame_pts_seconds
        # sorts by PRESENTATION time; those agree only without reordering. This
        # firmware emits none (no ctts box in any of 17,979 files), but rather
        # than assume it, reordering just disqualifies the stream-copy fast path
        # and the pair is re-encoded from decoded frames instead. Never a reason
        # to reject data.
        has_b_frames=int(s.get("has_b_frames", 0) or 0),
    )


def _pts_from(path: Path, entity: str) -> np.ndarray:
    out = subprocess.run(
        ["ffprobe", "-v", "error", "-select_streams", "v:0",
         "-show_entries", f"{entity}=pts_time", "-of", "csv=p=0", str(path)],
        capture_output=True, text=True, check=True,
    ).stdout
    vals = [float(x.split(",")[0]) for x in out.splitlines() if x.strip() and x[0].isdigit()]
    return np.sort(np.asarray(vals, dtype=np.float64))


def frame_pts_seconds(path: Path, expected_frames: int | None = None) -> np.ndarray:
    """Per-frame presentation timestamps, sorted into presentation order.

    Read from the container rather than synthesised from a nominal fps: the
    recorder is near-CFR but jitters ~+/-0.2 ms per frame, and inventing a
    perfect 30.000 Hz grid would slowly desynchronise frames from the IMU over
    a 600 s chunk.

    Packet timestamps are used, not frame timestamps. `-show_entries frame=`
    forces a full decode (~2 s per 15 s of 1080p HEVC -- ~80 s per camera per
    chunk, ~8 min per chunk across the six-camera rig); `-show_entries packet=`
    reads only the container index and is ~100x faster. Once sorted the two are
    identical here -- verified on this device's files -- because every packet
    holds exactly one frame. If the packet count ever disagrees with nb_frames
    we fall back to the slow authoritative path rather than publishing a
    timeline we cannot vouch for.
    """
    pts = _pts_from(path, "packet")
    if expected_frames is not None and len(pts) != expected_frames:
        return _pts_from(path, "frame")
    return pts


# ──────────────────────────────────────────────────────────────────────────────
# Stereo association + continuity
# ──────────────────────────────────────────────────────────────────────────────

@dataclass
class PairAssociation:
    """Which source frames form the emitted stereo pairs, and how well."""

    idx_left: np.ndarray     # source frame indices, strictly increasing
    idx_right: np.ndarray
    t_pair: np.ndarray       # device-clock time of each emitted pair (left eye)
    residual_ms: float       # worst |t_l - t_r| among emitted pairs
    residual_p95_ms: float
    modal_offset: int        # most common (i_right - i_left); 0 = natively aligned
    n_left_orphans: int
    n_right_orphans: int
    longest_gap_s: float
    coverage: float          # emitted pairs / frames the span should have held
    contiguous_prefix: bool  # True => both sides are 0..n-1 and remux can stream-copy


def associate_frames(t_left: np.ndarray, t_right: np.ndarray, dt: float,
                     tolerance_s: float = PAIR_MATCH_TOLERANCE_S) -> PairAssociation:
    """Pair frames by CAPTURE TIME, not by index.

    docs/06 invariant 1 requires this ("match frames by timestamp, drop
    unmatched edges"); pairing by index is what let a one-frame capture race
    (24% of eye pairs) and one-sided mid-clip drops reach rectification as
    stereo pairs that are not simultaneous.

    Mutual nearest neighbour inside +/-0.5 frame period, then a hard accept gate
    at `tolerance_s`. One pass handles both defect classes: a start race (left[i]
    finds right[i-1] at ~0.03 ms) and interior drops (the local index offset
    changes mid-clip and nearest-neighbour simply tracks it).
    """
    window = PAIR_MATCH_WINDOW_FRAMES * dt
    nl, nr = len(t_left), len(t_right)
    if nl == 0 or nr == 0:
        return PairAssociation(np.empty(0, int), np.empty(0, int), np.empty(0),
                               float("inf"), float("inf"), 0, nl, nr, 0.0, 0.0, False)

    # nearest right for each left, and vice versa
    j = np.searchsorted(t_right, t_left)
    cand = np.stack([np.clip(j - 1, 0, nr - 1), np.clip(j, 0, nr - 1)])
    d = np.abs(t_right[cand] - t_left[None, :])
    nn_r = cand[np.argmin(d, axis=0), np.arange(nl)]

    i = np.searchsorted(t_left, t_right)
    cand2 = np.stack([np.clip(i - 1, 0, nl - 1), np.clip(i, 0, nl - 1)])
    d2 = np.abs(t_left[cand2] - t_right[None, :])
    nn_l = cand2[np.argmin(d2, axis=0), np.arange(nr)]

    li = np.arange(nl)
    mutual = nn_l[nn_r] == li                       # both agree they are partners
    err = np.abs(t_right[nn_r] - t_left)
    # `window` bounds the search for a plausible partner; `tolerance_s` is the
    # quality gate that decides whether the partner is genuinely simultaneous.
    # tolerance_s is always the tighter of the two, so it decides the outcome.
    keep = mutual & (err <= window) & (err <= tolerance_s)

    idx_l = li[keep]
    idx_r = nn_r[keep]
    if idx_l.size == 0:
        return PairAssociation(np.empty(0, int), np.empty(0, int), np.empty(0),
                               float("inf"), float("inf"), 0, nl, nr, 0.0, 0.0, False)

    e = np.abs(t_right[idx_r] - t_left[idx_l])
    tp = t_left[idx_l]
    gaps = np.diff(tp)
    off = idx_r - idx_l
    span = float(tp[-1] - tp[0])
    expected = int(round(span / dt)) + 1 if span > 0 else len(tp)
    return PairAssociation(
        idx_left=idx_l, idx_right=idx_r, t_pair=tp,
        residual_ms=float(e.max() * 1e3),
        residual_p95_ms=float(np.percentile(e, 95) * 1e3),
        modal_offset=int(np.bincount(off - off.min()).argmax() + off.min()),
        n_left_orphans=int(nl - len(idx_l)),
        n_right_orphans=int(nr - len(idx_r)),
        longest_gap_s=float(gaps.max()) if gaps.size else 0.0,
        coverage=float(len(tp) / expected) if expected else 0.0,
        contiguous_prefix=bool(
            idx_l[0] == 0 and idx_r[0] == 0
            and np.array_equal(idx_l, np.arange(len(idx_l)))
            and np.array_equal(idx_r, np.arange(len(idx_r)))),
    )


def longest_contiguous_run(t: np.ndarray, max_gap_s: float) -> tuple[int, int, list[dict]]:
    """[start, stop) of the longest stretch with no gap larger than max_gap_s.

    A hole bigger than this is not one trajectory (see VIDEO_GAP_SPLIT_S), so a
    package must not span it. Emitting only the longest run is the conservative
    choice available inside today's one-package-per-chunk stage shape; the
    discarded spans are reported in chunk_report.json so nothing is lost
    silently.
    """
    if len(t) < 2:
        return 0, len(t), []
    brk = np.flatnonzero(np.diff(t) > max_gap_s)
    if brk.size == 0:
        return 0, len(t), []
    bounds = [0, *(brk + 1).tolist(), len(t)]
    runs = [(bounds[i], bounds[i + 1]) for i in range(len(bounds) - 1)]
    best = max(runs, key=lambda r: t[r[1] - 1] - t[r[0]])
    dropped = [{"from_s": float(t[a]), "to_s": float(t[b - 1]), "frames": int(b - a)}
               for a, b in runs if (a, b) != best]
    return best[0], best[1], dropped


def segment_coherence(starts: dict[str, float], dt: float) -> dict:
    """Are these six streams one recording?

    44 segments in the corpus hold TWO recordings ~3.5 h apart, uploaded under
    identical robocap_segment<N>_* filenames so the last writer won per name.
    The six streams are then not one capture and must never be paired.

    This has to run BEFORE the shared rebase origin is chosen: origin is
    min(imu_t0, *video_starts), so one stale aux tag 3.5 h early drags the whole
    package's timeline backwards even when the eye pair itself is fine.
    """
    vals = sorted(starts.values())
    spread = (vals[-1] - vals[0]) if vals else 0.0
    clusters: list[list[str]] = []
    cur: list[str] = []
    prev = None
    for name, t in sorted(starts.items(), key=lambda kv: kv[1]):
        if prev is not None and t - prev > SEGMENT_COHERENCE_FRAMES * dt:
            clusters.append(cur)
            cur = []
        cur.append(name)
        prev = t
    if cur:
        clusters.append(cur)
    return {
        "spread_s": float(spread),
        "spread_frames": float(spread / dt) if dt else 0.0,
        "n_clusters": len(clusters),
        "clusters": clusters,
        "coherent": spread <= SEGMENT_COHERENCE_FRAMES * dt,
        "warn": spread > SEGMENT_COHERENCE_WARN_FRAMES * dt,
        "threshold_s": SEGMENT_COHERENCE_FRAMES * dt,
    }


# ──────────────────────────────────────────────────────────────────────────────
# Extrinsic refinement
# ──────────────────────────────────────────────────────────────────────────────

# Frames are sampled across the WHOLE chunk, not from its head. A short window
# is often one scene -- on this footage the first 10 s is near-planar ground at
# close range, which is degenerate for epipolar geometry and sent the fit to a
# nonsense 15 deg. Scene diversity is what makes the problem well-posed.
# Half the frames FIT, half are held back to VALIDATE; scoring on the fitting
# data would only prove the optimiser ran, not that the geometry improved.
REFINE_FRAMES = 18
REFINE_MIN_MATCHES = 300
# Accept only a clear win. A marginal "improvement" is inside the noise of
# feature matching and is not worth departing from the vendor calibration for.
REFINE_MIN_IMPROVEMENT = 0.70
# A rig that shifted between calibration and recording moves by fractions of a
# degree (measured 0.63 deg here). Anything past this is not a calibration
# drift, it is a diverged fit on a degenerate scene -- reject it outright
# rather than hoping the held-out check catches it.
REFINE_MAX_DELTA_DEG = 3.0
# Tikhonov prior scale on the correction, in degrees: a delta this large costs
# the same as one pixel of row misalignment on every match. Without it the fit
# wanders down its ill-conditioned direction -- on the eye pair (a steep
# down-look at a close, near-planar scene) yaw about the vertical barely moves
# vertical disparity, and the unregularised fit ran to 3.9 deg of yaw for a 3%
# gain while discarding the genuine 0.6 deg correction.
#
# The right strength is pair-dependent and cannot be fixed in advance: measured
# held-out |dy| across the grid below is 2.32 px for eye at 0.2 but 6.60 px at
# 2.0, while front is 0.69 px at 2.0 and 1.20 px at 0.2. So it is SELECTED per
# pair on a validation split rather than guessed.
REFINE_PRIOR_GRID_DEG = (2.0, 1.0, 0.5, 0.3, 0.2, 0.1)


@dataclass
class ExtrinsicRefinement:
    """Outcome of the per-chunk extrinsic fit, published for provenance."""

    applied: bool
    method: str
    reason: str | None = None
    frames_fit: int = 0
    frames_validate: int = 0
    matches_fit: int = 0
    delta_rotation_rodrigues_deg: list[float] | None = None
    refined_rotation: list[list[float]] | None = None
    epipolar_px_before: float | None = None
    epipolar_px_after: float | None = None


def _rect_rotations(R: np.ndarray, T: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Bouguet rectifying rotations for the left and right cameras.

    Shared by the rectification maps and the extrinsic fit so the fit is
    optimising exactly the geometry that will be built, not an approximation
    of it.
    """
    r = cv2.Rodrigues(R)[0]
    R_l = cv2.Rodrigues(r / 2.0)[0]
    R_r = cv2.Rodrigues(-r / 2.0)[0]
    baseline_vec = R_l @ (-R.T @ T)
    e1 = baseline_vec / np.linalg.norm(baseline_vec)
    e2 = np.array([-e1[1], e1[0], 0.0])
    n2 = np.linalg.norm(e2)
    e2 = e2 / n2 if n2 > 1e-9 else np.array([0.0, 1.0, 0.0])
    R_rect = np.vstack([e1, e2, np.cross(e1, e2)])
    return R_rect @ R_l, R_rect @ R_r


def _rectified_dy_px(delta: np.ndarray, R0: np.ndarray, t: np.ndarray,
                     xl: np.ndarray, xr: np.ndarray, f: float) -> np.ndarray:
    """Vertical disparity, in rectified pixels, for each correspondence.

    THIS is what rectification quality means and what every downstream stereo
    consumer depends on: corresponding points must land on the same row. It is
    also far better conditioned than the Sampson epipolar distance, whose
    degenerate direction (yaw trading off against baseline direction on a close,
    near-planar scene) moves the fit a long way for no gain in row alignment.
    """
    R1, R2 = _rect_rotations(cv2.Rodrigues(delta)[0] @ R0, t)
    pl, pr = xl @ R1.T, xr @ R2.T
    # Points behind the rectified camera carry no meaning; keep them finite.
    zl = np.where(np.abs(pl[:, 2]) < 1e-9, 1e-9, pl[:, 2])
    zr = np.where(np.abs(pr[:, 2]) < 1e-9, 1e-9, pr[:, 2])
    return f * (pl[:, 1] / zl - pr[:, 1] / zr)


def _skew(t: np.ndarray) -> np.ndarray:
    return np.array([[0, -t[2], t[1]], [t[2], 0, -t[0]], [-t[1], t[0], 0]])


def _sampson(delta: np.ndarray, R0: np.ndarray, t: np.ndarray,
             xl: np.ndarray, xr: np.ndarray) -> np.ndarray:
    """Sampson epipolar distance in NORMALIZED image coords, per correspondence."""
    E = _skew(t) @ (cv2.Rodrigues(delta)[0] @ R0)
    num = np.einsum("ij,jk,ik->i", xr, E, xl)
    El, Er = E @ xl.T, E.T @ xr.T
    den = np.sqrt(El[0] ** 2 + El[1] ** 2 + Er[0] ** 2 + Er[1] ** 2) + 1e-12
    return num / den


def _fit_delta_rotation(R0: np.ndarray, t: np.ndarray,
                        xl: np.ndarray, xr: np.ndarray, f_eq: float,
                        prior_deg: float) -> np.ndarray:
    """Levenberg-Marquardt on 3 rotation parameters, Huber-weighted.

    ONLY the rotation is refined. The translation is left exactly as calibrated:
    imagery constrains the baseline DIRECTION but never its length, so fitting
    it would put metric stereo depth at the mercy of a scale the images cannot
    see. Rotation is also where the error actually is -- measured 0.68 deg on
    the eye pair and 1.10 deg (nearly pure roll) on front.

    Three unknowns and a smooth objective, so a hand-rolled LM with a numerical
    Jacobian converges in a few iterations -- not worth a scipy dependency that
    the runtime image does not carry.
    """
    n = len(xl)
    # Residuals are in PIXELS, so a delta of `prior_deg` costs the same as one
    # pixel of row misalignment on every match.
    prior_w = np.sqrt(n) / np.radians(prior_deg)

    def resid(d: np.ndarray) -> np.ndarray:
        return _rectified_dy_px(d, R0, t, xl, xr, f_eq)

    def cost(d: np.ndarray, w: np.ndarray) -> float:
        return float(np.sum(w * resid(d) ** 2) + np.sum((prior_w * d) ** 2))

    delta = np.zeros(3)
    lam = 1e-3
    for _ in range(60):
        r = resid(delta)
        # Huber weights, scale from the robust MAD of the current residuals.
        sigma = 1.4826 * np.median(np.abs(r - np.median(r))) + 1e-12
        k = 1.345 * sigma
        w = np.where(np.abs(r) <= k, 1.0, k / (np.abs(r) + 1e-12))
        J = np.empty((n, 3))
        for j in range(3):
            h = 1e-6
            step = np.zeros(3); step[j] = h
            J[:, j] = (resid(delta + step) - r) / h
        # Augment with the prior rows: residual prior_w*delta, Jacobian prior_w*I.
        Jw = J * w[:, None]
        A = Jw.T @ J + (prior_w ** 2) * np.eye(3) + lam * np.eye(3)
        g = Jw.T @ r + (prior_w ** 2) * delta
        try:
            step = np.linalg.solve(A, -g)
        except np.linalg.LinAlgError:
            break
        cand = delta + step
        if cost(cand, w) < cost(delta, w):
            delta, lam = cand, max(lam * 0.5, 1e-9)
        else:
            lam *= 4.0
        if np.linalg.norm(step) < 1e-11:
            break
    return delta


def _median_epipolar_px(delta: np.ndarray, R0: np.ndarray, t: np.ndarray,
                        xl: np.ndarray, xr: np.ndarray, f_eq: float) -> float:
    """Trimmed median |vertical disparity| after rectification, in pixels."""
    d = np.abs(_rectified_dy_px(delta, R0, t, xl, xr, f_eq))
    return float(np.median(d[d < np.percentile(d, 80)]))


def _collect_correspondences(
    left: Path, right: Path, calib, times_s: list[float],
    pair_index: list[tuple[int, int]] | None = None,
) -> list[tuple[np.ndarray, np.ndarray]]:
    """SIFT matches per sampled frame, undistorted to normalized coords."""
    sift = cv2.SIFT_create(nfeatures=8000)
    matcher = cv2.BFMatcher()
    out: list[tuple[np.ndarray, np.ndarray]] = []
    capL, capR = cv2.VideoCapture(str(left)), cv2.VideoCapture(str(right))
    try:
        for j, ts in enumerate(times_s):
            # Seek by ASSOCIATED FRAME INDEX, never by wall time. Both eyes'
            # files start at different instants on the device clock (measured:
            # 737 of 2,972 eye pairs = 24.8% differ by exactly one frame
            # period), so seeking both to the same file-relative msec pairs
            # frames captured ~33 ms apart. At this device's rates that is
            # ~1 deg of head motion and ~11.7 px of apparent dy at the
            # rectified focal length -- LARGER than the 5-10 px epipolar error
            # the refinement exists to remove. The fit then absorbs head motion
            # into the rig extrinsic, which corrupts disparity and metric scale
            # while leaving gravity self-consistent, so nothing downstream
            # notices. refine_extrinsics: true is set for bitrobot in
            # pipeline.yaml -- this path is live.
            if pair_index is not None:
                il, ir = pair_index[j]
                capL.set(cv2.CAP_PROP_POS_FRAMES, float(il))
                capR.set(cv2.CAP_PROP_POS_FRAMES, float(ir))
            else:
                capL.set(cv2.CAP_PROP_POS_MSEC, ts * 1000.0)
                capR.set(cv2.CAP_PROP_POS_MSEC, ts * 1000.0)
            okL, fL = capL.read()
            okR, fR = capR.read()
            if not (okL and okR):
                continue
            kL, dL = sift.detectAndCompute(cv2.cvtColor(fL, cv2.COLOR_BGR2GRAY), None)
            kR, dR = sift.detectAndCompute(cv2.cvtColor(fR, cv2.COLOR_BGR2GRAY), None)
            if dL is None or dR is None or len(kL) < 20 or len(kR) < 20:
                continue
            good = [m for m, n in matcher.knnMatch(dL, dR, k=2) if m.distance < 0.7 * n.distance]
            if len(good) < 30:
                continue
            pL = np.array([kL[m.queryIdx].pt for m in good], dtype=np.float64)
            pR = np.array([kR[m.trainIdx].pt for m in good], dtype=np.float64)
            nL = cv2.fisheye.undistortPoints(
                pL.reshape(-1, 1, 2), calib.K1, calib.D1.reshape(4, 1)
            ).reshape(-1, 2)
            nR = cv2.fisheye.undistortPoints(
                pR.reshape(-1, 1, 2), calib.K2, calib.D2.reshape(4, 1)
            ).reshape(-1, 2)
            # Drop gross mismatches BEFORE the fit, using epipolar geometry
            # estimated from the points themselves. A Huber loss alone cannot
            # absorb SIFT's outright wrong matches, and doing this against the
            # vendor model instead would bias the result toward the very
            # calibration under correction.
            _, inl = cv2.findEssentialMat(
                nL, nR, np.eye(3), method=cv2.RANSAC, prob=0.999, threshold=3e-3
            )
            if inl is None:
                continue
            keep = inl.ravel().astype(bool)
            if keep.sum() < 30:
                continue
            nL, nR = nL[keep], nR[keep]
            out.append((
                np.hstack([nL, np.ones((len(nL), 1))]),
                np.hstack([nR, np.ones((len(nR), 1))]),
            ))
    finally:
        capL.release(); capR.release()
    return out


def refine_extrinsics(
    left: Path, right: Path, calib, duration_s: float, logger: logging.Logger,
    n_frames: int = REFINE_FRAMES,
    idx_left: np.ndarray | None = None, idx_right: np.ndarray | None = None,
    t_pair: np.ndarray | None = None,
) -> ExtrinsicRefinement:
    """Fit a static rotation correction for one stereo pair from its own footage.

    The shipped calibration was captured in a DIFFERENT session from the
    recordings (2026-08-18 20:58 vs data from 2026-08-17 onward) and its
    `calibration_quality` block understates the real error by ~10-20x: measured
    5.12 px (eye) and 9.79 px (front) median epipolar residual against a claimed
    ~0.5 px. Nearly all of it is one static rotation error, so it is removable.

    The vendor rotation is never overwritten -- `raw.rotation` in
    calibration.json keeps it. The correction is applied when BUILDING the
    rectification maps, and the delta, the refined matrix and the before/after
    numbers are published in the `extrinsic_refinement` block so the change is
    auditable and reversible.

    Refuses to apply anything it cannot show is better on HELD-OUT frames.
    """
    span = max(duration_s, 2.0)
    pair_index = None
    if idx_left is not None and idx_right is not None and len(idx_left) >= n_frames:
        # Sample evenly across the ASSOCIATION, so every correspondence pair is
        # two frames that are simultaneous to <=1 ms by construction.
        pick = np.linspace(0, len(idx_left) - 1, n_frames).round().astype(int)
        pair_index = [(int(idx_left[k]), int(idx_right[k])) for k in pick]
        times = [float(t_pair[k] - t_pair[0]) if t_pair is not None else 0.0 for k in pick]
    else:
        times = list(np.linspace(0.02 * span, 0.98 * span, n_frames))
    per_frame = _collect_correspondences(left, right, calib, times, pair_index)
    if len(per_frame) < 6:
        return ExtrinsicRefinement(
            applied=False, method="sampson_lm_rotation_only",
            reason=f"only {len(per_frame)} usable frames across {span:.0f}s "
                   f"(correspondence_source="
                   f"{'association' if pair_index is not None else 'wallclock'})",
        )

    # THREE-way split, by FRAME rather than by correspondence: matches inside
    # one frame share a scene and are not independent, so a random split would
    # leak. `fit` trains, `select` chooses the prior strength, and `report` is
    # never touched until the final accept/reject -- otherwise picking the prior
    # on the same frames we report would quietly turn the held-out number into
    # a training number.
    fit, select, report_set = per_frame[0::3], per_frame[1::3], per_frame[2::3]

    def stack(group):
        return (np.vstack([a for a, _ in group]), np.vstack([b for _, b in group]))

    xl_f, xr_f = stack(fit)
    xl_s, xr_s = stack(select)
    xl_r, xr_r = stack(report_set)
    if min(len(xl_f), len(xl_s), len(xl_r)) < REFINE_MIN_MATCHES:
        return ExtrinsicRefinement(
            applied=False, method="rectified_dy_lm_rotation_only",
            reason=(f"too few matches (fit={len(xl_f)}, select={len(xl_s)}, "
                    f"report={len(xl_r)})"),
        )

    R0 = np.asarray(calib.R, dtype=np.float64)
    t = np.asarray(calib.T, dtype=np.float64).reshape(3)
    f_eq = float((calib.K1[0, 0] + calib.K1[1, 1]) / 2)

    best_delta, best_prior, best_select = np.zeros(3), None, float("inf")
    for prior in REFINE_PRIOR_GRID_DEG:
        cand = _fit_delta_rotation(R0, t, xl_f, xr_f, f_eq, prior)
        if float(np.linalg.norm(np.degrees(cand))) > REFINE_MAX_DELTA_DEG:
            continue
        score = _median_epipolar_px(cand, R0, t, xl_s, xr_s, f_eq)
        if score < best_select:
            best_delta, best_prior, best_select = cand, prior, score

    delta = best_delta
    before = _median_epipolar_px(np.zeros(3), R0, t, xl_r, xr_r, f_eq)
    after = _median_epipolar_px(delta, R0, t, xl_r, xr_r, f_eq)

    common = dict(
        method="rectified_dy_lm_rotation_only",
        frames_fit=len(fit), frames_validate=len(report_set), matches_fit=int(len(xl_f)),
        delta_rotation_rodrigues_deg=[float(v) for v in np.degrees(delta)],
        epipolar_px_before=before, epipolar_px_after=after,
    )
    if best_prior is None:
        return ExtrinsicRefinement(
            applied=False,
            reason=f"every candidate fit exceeded the {REFINE_MAX_DELTA_DEG} deg bound",
            **common,
        )
    if after > before * REFINE_MIN_IMPROVEMENT:
        logger.warning(
            f"extrinsic refinement REJECTED: held-out |dy| {before:.2f} -> {after:.2f} px "
            f"is not a clear win; keeping the vendor calibration"
        )
        return ExtrinsicRefinement(
            applied=False,
            reason=f"insufficient improvement ({before:.2f} -> {after:.2f} px)",
            **common,
        )

    R_ref = cv2.Rodrigues(delta)[0] @ R0
    logger.info(
        f"extrinsic refinement APPLIED: held-out |dy| {before:.2f} -> {after:.2f} px "
        f"via {np.linalg.norm(np.degrees(delta)):.3f} deg rotation "
        f"(prior {best_prior} deg)"
    )
    return ExtrinsicRefinement(
        applied=True,
        refined_rotation=[[float(v) for v in row] for row in R_ref],
        **common,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Rectification
# ──────────────────────────────────────────────────────────────────────────────
#
# The camera comes from cv2.fisheye.stereoRectify, zoomed in just enough that no
# output pixel falls outside its raw image (OpenCV's own camera, even at
# balance 0, can leave a thin black sliver at the frame edge). Falls back to a
# Bouguet split-rotation construction, built by hand, when OpenCV cannot
# produce a camera for the pair: on a lens model that folds back inside the
# image (d theta_d / d theta <= 0), cv2.fisheye.undistortPoints cannot invert
# the polynomial and stereoRectify returns f = 0 -- this rig's front pair hits
# exactly that on the vendor calibration (9.26 deg inter-camera roll is not the
# cause; the polynomial folds before the image edge). The rectifying rotations
# R1/R2 are the same construction either way, so row alignment is unaffected.

@dataclass
class RectMaps:
    map1_left: np.ndarray
    map2_left: np.ndarray
    map1_right: np.ndarray
    map2_right: np.ndarray
    R1: np.ndarray
    R2: np.ndarray
    P1: np.ndarray
    P2: np.ndarray
    Q: np.ndarray
    baseline_m: float


class RectificationDegenerate(ValueError):
    """cv2.fisheye.stereoRectify gave no usable camera, or one below the field spec."""


@dataclass
class EyeDiagnostics:
    """What one eye's rectification maps imply, measured before encoding."""

    hfov_deg: float
    vfov_deg: float
    invalid_fraction: float          # output pixels mapped outside the source image
    max_sampled_angle_deg: float     # widest raw-camera angle any output pixel uses
    lens_fold_deg: float | None      # where the KB4 polynomial turns over, if it does
    lens_valid: bool
    lens_reason: str | None


# OpenCV's balance: 0 crops to pixels the source actually covers, 1 keeps
# every source pixel (black corners). 0 is the posture the Bouguet fallback
# also aims for.
RECTIFY_BALANCE = 0.0

# A lens model is only a lens over the angles where it stays physical. These
# match the border-detection QC's lens check (docs/methodology.md there):
# rectilinear (tan) and orthographic (sin) bound every real lens, with 2% slack;
# an equidistant fisheye stays within 10% of stereographic (2 tan(t/2)).
LENS_PHYSICAL_TOLERANCE = 0.02
LENS_FISHEYE_FAMILY_TOLERANCE = 0.10
LENS_ENVELOPE_MIN_DEG = 10.0

# Any rectified field outside this range, on either axis, is not a camera
# anyone asked for; it only arises from a lens model OpenCV cannot invert.
RECTIFY_SANE_FOV_DEG = (30.0, 170.0)

# The customer's floor for the rectified field (2.4 Camera Performance: >=110
# deg H and >=80 deg V), the same one mcap_export reports against and the reason
# rectified_hfov_degrees is 114. OpenCV picks its own field and, zoomed to cover
# the frame, can land under it on a poor lens model; such a camera falls back to
# the Bouguet construction, which is built to rectified_hfov_degrees.
RECTIFY_SPEC_MIN_FOV_DEG = (110.0, 80.0)      # (horizontal, vertical)

# balance = 0 does not mean "no border". OpenCV chooses each eye's camera from
# the four mid-edge points of its frame, then keeps the SMALLER focal length of
# the two and averages their principal points, so the output's corners and
# edges can still reach past the raw image. The zoom that removes that sliver
# keeps every sample this far inside the raw frame: the adapter remaps with
# INTER_CUBIC, which reads one pixel before and two after each sample, and
# BORDER_CONSTANT (black), so a sample nearer the edge darkens the output's
# edge pixels.
RECTIFY_COVER_MARGIN_PX = 2.0
RECTIFY_COVER_TOLERANCE = 1e-4        # relative precision of the zoom
# A sliver needs a percent or two of zoom. Needing more means OpenCV's camera
# is not a crop of this lens at all.
RECTIFY_COVER_MAX_ZOOM = 1.5


def _kb4_radius(theta: np.ndarray, D: np.ndarray) -> np.ndarray:
    k1, k2, k3, k4 = [float(v) for v in np.asarray(D).ravel()[:4]]
    t2 = theta * theta
    return theta * (1 + k1 * t2 + k2 * t2 ** 2 + k3 * t2 ** 3 + k4 * t2 ** 4)


def kb4_fold_deg(D: np.ndarray) -> float | None:
    """First incidence angle at which the KB4 polynomial stops increasing."""
    k1, k2, k3, k4 = [float(v) for v in np.asarray(D).ravel()[:4]]
    theta = np.radians(np.linspace(0.0, 89.0, 17801))
    t2 = theta * theta
    slope = 1 + 3 * k1 * t2 + 5 * k2 * t2 ** 2 + 7 * k3 * t2 ** 3 + 9 * k4 * t2 ** 4
    turned = np.nonzero(slope <= 0)[0]
    return float(np.degrees(theta[turned[0]])) if len(turned) else None


def max_sampled_angle_deg(P: np.ndarray, R: np.ndarray, w: int, h: int) -> float:
    """Widest angle, in the raw camera, that any rectified pixel samples.

    Rays through the border of the rectified frame are taken back through the
    rectifying rotation (x_rect = R x_raw, so x_raw = R^T x_rect).
    """
    edge = np.linspace(0.0, 1.0, 256)
    u = np.concatenate([edge * (w - 1), np.full(256, w - 1.0), edge * (w - 1), np.zeros(256)])
    v = np.concatenate([np.zeros(256), edge * (h - 1), np.full(256, h - 1.0), edge * (h - 1)])
    K = P[:3, :3]
    rays = np.linalg.inv(K) @ np.vstack([u, v, np.ones_like(u)])
    raw = R.T @ rays
    return float(np.degrees(np.arctan2(np.hypot(raw[0], raw[1]), raw[2])).max())


def lens_validity(D: np.ndarray, used_deg: float) -> tuple[float | None, bool, str | None]:
    """(fold angle, valid, reason) for an equidistant lens model over [0, used_deg]."""
    fold = kb4_fold_deg(D)
    if fold is not None and used_deg > fold:
        return fold, False, f"lens model folds at {fold:.1f} deg, maps sample to {used_deg:.1f} deg"
    if used_deg <= LENS_ENVELOPE_MIN_DEG:
        return fold, True, None
    theta = np.radians(np.linspace(LENS_ENVELOPE_MIN_DEG, used_deg, 400))
    r = _kb4_radius(theta, D)
    vs_rect = float((r / np.tan(theta)).max())
    vs_ortho = float((r / np.sin(theta)).min())
    vs_stereo = float((r / (2 * np.tan(theta / 2))).max())
    if vs_rect > 1 + LENS_PHYSICAL_TOLERANCE:
        return fold, False, f"lens model {vs_rect:.2f}x more expansive than any lens"
    if vs_ortho < 1 - LENS_PHYSICAL_TOLERANCE:
        return fold, False, f"lens model more compressive than any lens ({vs_ortho:.2f}x)"
    if vs_stereo > 1 + LENS_FISHEYE_FAMILY_TOLERANCE:
        return fold, False, f"declared fisheye, {vs_stereo:.2f}x past the stereographic bound"
    return fold, True, None


def invalid_fraction(map1: np.ndarray, map2: np.ndarray, w: int, h: int,
                     margin: float = 0.0) -> float:
    """Share of output pixels whose source lies outside the image: the black border.

    With a margin, a source within that many pixels of the image edge counts
    as outside too.
    """
    ok = (np.isfinite(map1) & np.isfinite(map2)
          & (map1 >= margin) & (map1 <= w - 1 - margin)
          & (map2 >= margin) & (map2 <= h - 1 - margin))
    return float(1.0 - ok.mean())


def pinhole_fov(P: np.ndarray, w: int, h: int) -> tuple[float, float, float]:
    """Horizontal, vertical and diagonal field of the pinhole camera P, in degrees.

    The frame's edges sit half a pixel outside its first and last pixel
    centres. Holds for a principal point off the frame centre, which is where
    OpenCV puts it (v1's fixed Bouguet camera is centred, where this agrees
    with the simpler focal-length-only formula).
    """
    f, cx, cy = float(P[0, 0]), float(P[0, 2]), float(P[1, 2])
    if cx == (w - 1) / 2.0 and cy == (h - 1) / 2.0:
        # Centred (the Bouguet fallback's camera): the closed form, so its
        # calibration.json carries bit-for-bit the values it always has.
        return (float(2 * np.degrees(np.arctan(w / (2 * f)))),
                float(2 * np.degrees(np.arctan(h / (2 * f)))),
                float(2 * np.degrees(np.arctan(np.hypot(w / 2, h / 2) / f))))
    left, right = (cx + 0.5) / f, (w - 0.5 - cx) / f
    top, bottom = (cy + 0.5) / f, (h - 0.5 - cy) / f

    def angle(a: tuple[float, float], b: tuple[float, float]) -> float:
        ra, rb = np.array([a[0], a[1], 1.0]), np.array([b[0], b[1], 1.0])
        cos = float(ra @ rb) / float(np.linalg.norm(ra) * np.linalg.norm(rb))
        return float(np.degrees(np.arccos(np.clip(cos, -1.0, 1.0))))

    hf = float(np.degrees(np.arctan(left) + np.arctan(right)))
    vf = float(np.degrees(np.arctan(top) + np.arctan(bottom)))
    df = max(angle((-left, -top), (right, bottom)), angle((right, -top), (-left, bottom)))
    return hf, vf, df


def eye_diagnostics(D: np.ndarray, P: np.ndarray, R: np.ndarray,
                    map1: np.ndarray, map2: np.ndarray, w: int, h: int) -> EyeDiagnostics:
    hf, vf, _ = pinhole_fov(P, w, h)
    used = max_sampled_angle_deg(P, R, w, h)
    fold, ok, reason = lens_validity(D, used)
    return EyeDiagnostics(
        hfov_deg=hf, vfov_deg=vf,
        invalid_fraction=invalid_fraction(map1, map2, w, h),
        max_sampled_angle_deg=used, lens_fold_deg=fold,
        lens_valid=ok, lens_reason=reason,
    )


def rectification_report(calib, maps: RectMaps, method: str) -> dict:
    """Per-eye diagnostics of the built maps, for chunk_report.json."""
    w, h = calib.width, calib.height
    left = eye_diagnostics(calib.D1, maps.P1, maps.R1, maps.map1_left, maps.map2_left, w, h)
    right = eye_diagnostics(calib.D2, maps.P2, maps.R2, maps.map1_right, maps.map2_right, w, h)
    return {"method": method, "left": vars(left), "right": vars(right)}


def _raw_pixels(K: np.ndarray, D: np.ndarray, R: np.ndarray, f: float, cx: float, cy: float,
                u: np.ndarray, v: np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """Where output pixels (u, v) sample the raw image.

    The arithmetic of cv2.fisheye.initUndistortRectifyMap for the pinhole
    camera (f, cx, cy) behind rotation R: a ray behind the camera samples
    nothing (NaN).
    """
    x, y, z = R.T @ np.vstack([(u - cx) / f, (v - cy) / f, np.ones_like(u)])
    with np.errstate(divide="ignore", invalid="ignore"):
        a, b = x / z, y / z
        r = np.hypot(a, b)
        scale = np.where(r > 0, _kb4_radius(np.arctan(r), D) / r, 1.0)
    mu = K[0, 0] * a * scale + K[0, 1] * b * scale + K[0, 2]
    mv = K[1, 1] * b * scale + K[1, 2]
    behind = z <= 0
    mu[behind] = np.nan
    mv[behind] = np.nan
    return mu, mv


def _cover_zoom(K1: np.ndarray, D1: np.ndarray, R1: np.ndarray,
                K2: np.ndarray, D2: np.ndarray, R2: np.ndarray,
                f: float, cx: float, cy: float, w: int, h: int) -> float:
    """Smallest factor >= 1 on f that keeps both eyes' frames inside their raw images.

    Every pixel on the border of the output frame must sample at least
    RECTIFY_COVER_MARGIN_PX inside the raw image. Zooming in about the
    principal point pulls every ray toward the axis; build_rectification_maps
    then confirms the zoom on every pixel of the full maps.
    """
    u = np.concatenate([np.arange(w), np.full(h, w - 1), np.arange(w), np.zeros(h)]).astype(np.float64)
    v = np.concatenate([np.zeros(w), np.arange(h), np.full(w, h - 1), np.arange(h)]).astype(np.float64)

    def covered(zoom: float) -> bool:
        for K, D, R in ((K1, D1, R1), (K2, D2, R2)):
            mu, mv = _raw_pixels(K, D, R, f * zoom, cx, cy, u, v)
            with np.errstate(invalid="ignore"):
                inside = ((mu >= RECTIFY_COVER_MARGIN_PX) & (mu <= w - 1 - RECTIFY_COVER_MARGIN_PX)
                          & (mv >= RECTIFY_COVER_MARGIN_PX) & (mv <= h - 1 - RECTIFY_COVER_MARGIN_PX))
            if not inside.all():
                return False
        return True

    if covered(1.0):
        return 1.0
    lo, hi = 1.0, 1.01
    while not covered(hi):
        lo, hi = hi, hi * 1.1
        if hi > RECTIFY_COVER_MAX_ZOOM:
            raise RectificationDegenerate(
                f"covering the frame needs more than {RECTIFY_COVER_MAX_ZOOM}x zoom on OpenCV's camera")
    while hi / lo - 1.0 > RECTIFY_COVER_TOLERANCE:
        mid = 0.5 * (lo + hi)
        if covered(mid):
            hi = mid
        else:
            lo = mid
    return hi


def _opencv_rectify(K1, D1, K2, D2, size, R, T):
    R1, R2, P1, P2, Q = cv2.fisheye.stereoRectify(
        K1, D1, K2, D2, size, R, T,
        flags=cv2.CALIB_ZERO_DISPARITY, newImageSize=size,
        balance=RECTIFY_BALANCE, fov_scale=1.0,
    )
    f = float(P1[0, 0])
    if not (np.isfinite(P1).all() and np.isfinite(P2).all() and f > 1.0
            and abs(float(P2[0, 0]) - f) < 1e-6):
        raise RectificationDegenerate(
            f"cv2.fisheye.stereoRectify returned f={f:.3f} (P1={P1[:, :3].tolist()}); "
            f"the usual cause is a lens model that folds back inside the image")
    # A usable f can still come with a camera no one wants: on a lens model
    # that folds at 76 deg, OpenCV returned a 15 x 110 deg field with the
    # principal point far outside the frame.
    w, h = size
    cx, cy = float(P1[0, 2]), float(P1[1, 2])
    hf, vf, _ = pinhole_fov(P1, w, h)
    if not (0 <= cx < w and 0 <= cy < h and RECTIFY_SANE_FOV_DEG[0] <= hf <= RECTIFY_SANE_FOV_DEG[1]
            and RECTIFY_SANE_FOV_DEG[0] <= vf <= RECTIFY_SANE_FOV_DEG[1]):
        raise RectificationDegenerate(
            f"cv2.fisheye.stereoRectify returned a {hf:.1f} x {vf:.1f} deg field with "
            f"principal point ({cx:.0f}, {cy:.0f}) on a {w}x{h} image")
    return R1, R2, P1, P2, Q


def _rect_maps_from_camera(K1, D1, K2, D2, R1, R2, f: float, cx: float, cy: float,
                           baseline: float, w: int, h: int) -> RectMaps:
    # With CALIB_ZERO_DISPARITY both cameras share one principal point and
    # P2[0,3] = -f * baseline (Q[3,2] = +1/baseline; see the Bouguet fallback's
    # docstring for the sign convention).
    P1 = np.array([[f, 0, cx, 0], [0, f, cy, 0], [0, 0, 1, 0]], dtype=np.float64)
    P2 = np.array([[f, 0, cx, -f * baseline], [0, f, cy, 0], [0, 0, 1, 0]], dtype=np.float64)
    Q = np.array([[1, 0, 0, -cx], [0, 1, 0, -cy], [0, 0, 0, f],
                  [0, 0, 1.0 / baseline, 0]], dtype=np.float64)
    m1l, m2l = cv2.fisheye.initUndistortRectifyMap(K1, D1, R1, P1[:, :3], (w, h), cv2.CV_32FC1)
    m1r, m2r = cv2.fisheye.initUndistortRectifyMap(K2, D2, R2, P2[:, :3], (w, h), cv2.CV_32FC1)
    return RectMaps(m1l, m2l, m1r, m2r, R1, R2, P1, P2, Q, baseline)


def _build_rectification_maps_opencv(
    calib, logger: logging.Logger, rotation_override: np.ndarray | None, details: dict,
) -> RectMaps:
    """OpenCV's camera for the pair, zoomed just enough to leave no border.

    Raises RectificationDegenerate when OpenCV cannot produce a camera for the
    pair, or when the covered camera's field is below RECTIFY_SPEC_MIN_FOV_DEG;
    the caller falls back to the Bouguet construction.
    """
    K1, D1 = calib.K1, np.asarray(calib.D1, dtype=np.float64).reshape(4, 1)
    K2, D2 = calib.K2, np.asarray(calib.D2, dtype=np.float64).reshape(4, 1)
    R = np.asarray(calib.R if rotation_override is None else rotation_override,
                   dtype=np.float64)
    T = np.asarray(calib.T, dtype=np.float64).reshape(3, 1)
    w, h = calib.width, calib.height

    R1, R2, P1, P2, _ = _opencv_rectify(K1, D1, K2, D2, (w, h), R, T)
    f = float(P1[0, 0])
    cx, cy = float(P1[0, 2]), float(P1[1, 2])
    baseline = float(-P2[0, 3] / f)
    if baseline <= 0:
        raise RectificationDegenerate(
            f"rectified baseline came out {baseline * 1000:.2f} mm; the right camera "
            f"must sit at +x of the left in the rectified frame")

    zoom = _cover_zoom(K1, D1, R1, K2, D2, R2, f, cx, cy, w, h)
    maps = _rect_maps_from_camera(K1, D1, K2, D2, R1, R2, f * zoom, cx, cy, baseline, w, h)
    # _cover_zoom checks the frame's border pixels; confirm on every pixel.
    for _ in range(50):
        if max(invalid_fraction(maps.map1_left, maps.map2_left, w, h, RECTIFY_COVER_MARGIN_PX),
               invalid_fraction(maps.map1_right, maps.map2_right, w, h, RECTIFY_COVER_MARGIN_PX)) == 0.0:
            break
        zoom *= 1.001
        maps = _rect_maps_from_camera(K1, D1, K2, D2, R1, R2, f * zoom, cx, cy, baseline, w, h)
    else:
        logger.warning("rectification: border pixels remain after zooming to cover the frame")
    details.update({"opencv_focal_px": f, "cover_zoom": zoom})
    hf, vf, _ = pinhole_fov(maps.P1, w, h)
    min_h, min_v = RECTIFY_SPEC_MIN_FOV_DEG
    if hf < min_h or vf < min_v:
        raise RectificationDegenerate(
            f"OpenCV's border-free field is {hf:.1f} x {vf:.1f} deg, below the "
            f"{min_h:g} x {min_v:g} deg spec")
    logger.info(
        f"rectification (cv2.fisheye.stereoRectify, balance={RECTIFY_BALANCE}): "
        f"f={f * zoom:.2f}px (OpenCV {f:.2f}px x {zoom:.4f} to cover the frame) "
        f"c=({cx:.1f},{cy:.1f}) fov={hf:.1f}x{vf:.1f} deg baseline={baseline * 1000:.2f}mm"
    )
    return maps


def _build_rectification_maps_bouguet(
    calib,
    logger: logging.Logger,
    target_hfov_deg: float = DEFAULT_RECTIFIED_HFOV_DEG,
    rotation_override: np.ndarray | None = None,
) -> RectMaps:
    """Rectification maps for one fisheye stereo pair, built by hand.

    Fallback used when cv2.fisheye.stereoRectify degenerates: on this rig's
    front pair, on the vendor calibration, it returns P1[0,0] == P2[0,0] == 0.0
    at every `balance` in [0,1] (an all-black image, 0.2% non-zero pixels) --
    the KB4 polynomial folds back before the raw image edge. The construction
    below handles both pairs and reaches the calibration's own epipolar noise
    floor, at a fixed pinhole field rather than whatever OpenCV's camera
    would have given.

    Steps: split the relative rotation in half so both image planes become
    parallel; rotate so the baseline lies along +X (the definition of a
    rectified pair); give both cameras one shared pinhole intrinsic with
    -f*baseline in P2 so disparity is zero at infinity.
    """
    K1, D1 = calib.K1, np.asarray(calib.D1, dtype=np.float64).reshape(4, 1)
    K2, D2 = calib.K2, np.asarray(calib.D2, dtype=np.float64).reshape(4, 1)
    # rotation_override is the refined extrinsic rotation when one was measured
    # and validated; otherwise the vendor value is used unchanged.
    R = np.asarray(calib.R if rotation_override is None else rotation_override,
                   dtype=np.float64)
    T = np.asarray(calib.T, dtype=np.float64).reshape(3)
    w, h = calib.width, calib.height

    # Left frame is the reference, so the right camera's orientation in it is R.
    # Sending left through +r/2 and right through -r/2 lands both on the common
    # orientation Rodrigues(r/2). Reversing these two signs tilts the pair by the
    # full inter-camera angle instead of cancelling it.
    # p_r = R p_l + T puts the right camera centre at -R^T T in the left frame;
    # _rect_rotations does the half-rotation split and the baseline alignment.
    R1, R2 = _rect_rotations(R, T)
    baseline_vec = cv2.Rodrigues(cv2.Rodrigues(R)[0] / 2.0)[0] @ (-R.T @ T)
    f = (w / 2.0) / np.tan(np.radians(target_hfov_deg) / 2.0)
    cx, cy = (w - 1) / 2.0, (h - 1) / 2.0
    baseline = float(np.linalg.norm(baseline_vec))

    P1 = np.array([[f, 0, cx, 0], [0, f, cy, 0], [0, 0, 1, 0]], dtype=np.float64)
    P2 = np.array([[f, 0, cx, -f * baseline], [0, f, cy, 0], [0, 0, 1, 0]], dtype=np.float64)
    # Q maps (u, v, disparity, 1) -> (X, Y, Z, W). Q[3][2] is +1/baseline:
    # with -1/baseline every reprojected point comes back negated in X, Y and Z.
    Q = np.array([[1, 0, 0, -cx], [0, 1, 0, -cy], [0, 0, 0, f],
                  [0, 0, 1.0 / baseline, 0]], dtype=np.float64)

    m1l, m2l = cv2.fisheye.initUndistortRectifyMap(K1, D1, R1, P1[:, :3], (w, h), cv2.CV_32FC1)
    m1r, m2r = cv2.fisheye.initUndistortRectifyMap(K2, D2, R2, P2[:, :3], (w, h), cv2.CV_32FC1)

    logger.info(
        f"rectification (Bouguet fallback): hfov={target_hfov_deg:.1f} deg f={f:.2f}px "
        f"baseline={baseline * 1000:.2f}mm"
    )
    return RectMaps(m1l, m2l, m1r, m2r, R1, R2, P1, P2, Q, baseline)


def build_rectification_maps(
    calib,
    logger: logging.Logger,
    target_hfov_deg: float = DEFAULT_RECTIFIED_HFOV_DEG,
    rotation_override: np.ndarray | None = None,
    details: dict | None = None,
) -> RectMaps:
    """Rectification maps for one fisheye stereo pair.

    Tries cv2.fisheye.stereoRectify first (see the module-level comment above
    RectMaps); falls back to the Bouguet construction when OpenCV degenerates
    or its covered field is below RECTIFY_SPEC_MIN_FOV_DEG. `target_hfov_deg`
    only affects the fallback -- OpenCV chooses the field itself. `details`,
    when given, is filled in place with the method used and, for the OpenCV
    path, its own focal length and the zoom applied; on a fallback it also
    gets `degenerate_reason`.
    """
    out: dict = {} if details is None else details
    try:
        maps = _build_rectification_maps_opencv(calib, logger, rotation_override, out)
        out["method"] = "cv2.fisheye.stereoRectify + cover"
    except Exception as exc:  # noqa: BLE001 -- any failure of the OpenCV path falls back
        # The Bouguet construction is what this adapter has always shipped, so
        # falling back to it on ANY error keeps every pair that rectified
        # before rectifying now; a cv2.error from stereoRectify must not fail
        # a chunk the old code handled.
        reason = exc if isinstance(exc, RectificationDegenerate) else f"{type(exc).__name__}: {exc}"
        logger.warning(f"rectification degenerate ({reason}); falling back to the Bouguet construction")
        for key in ("opencv_focal_px", "cover_zoom"):
            out.pop(key, None)
        maps = _build_rectification_maps_bouguet(calib, logger, target_hfov_deg, rotation_override)
        out["method"] = "bouguet_fallback"
        out["degenerate_reason"] = str(reason)
    return maps


def source_fov_deg(
    K: np.ndarray, D: np.ndarray, w: int, h: int, strict: bool = True
) -> tuple[float, float, float]:
    """True FOV of a Kannala-Brandt source camera.

    Inverts the actual KB4 polynomial r(th) = f*(th + k1 th^3 + ... + k4 th^9)
    numerically. The pinhole 2*atan(w/2f) formula is meaningless for a fisheye
    and would under-report this rig's field by ~40 degrees.

    These are MODEL EXTRAPOLATIONS, not measurements. The 9th-order polynomial
    is fitted over the checkerboard's angular support and is not trustworthy
    beyond it -- on this rig the two cameras of the same `eye` pair, same lens
    part, differ by 16 deg (145.0 vs 128.7). Worse, for front/cam1 the
    polynomial is NON-MONOTONIC: r(th) turns over at 85.9 deg (r_max 1046.2 px)
    while the image corner needs 1109.2 px. Naively clamping there returns
    exactly 180.00 deg, a physically impossible number that was being published
    into calibration.json. With strict=True that condition raises; the caller
    decides whether to degrade or fail.
    """
    k1, k2, k3, k4 = [float(v) for v in np.asarray(D).ravel()[:4]]
    th = np.linspace(0.0, np.pi / 2, 20001)

    def theta_at(r_px: float, f: float, axis: str) -> float:
        r = f * (th + k1 * th**3 + k2 * th**5 + k3 * th**7 + k4 * th**9)
        r_mono = np.maximum.accumulate(r)
        if r_px > r_mono[-1] or r_px > r.max():
            msg = (
                f"KB4 model cannot reach r={r_px:.1f}px on {axis} "
                f"(polynomial peaks at {r.max():.1f}px, turns over at "
                f"{np.degrees(th[int(np.argmax(r))]):.1f} deg). The FOV here would be a "
                f"clamped extrapolation, not a measurement."
            )
            if strict:
                raise ValueError(msg)
            return float("nan")
        return float(np.interp(r_px, r_mono, th))

    fx, fy = float(K[0, 0]), float(K[1, 1])
    cx, cy = float(K[0, 2]), float(K[1, 2])
    rx, ry = max(cx, w - cx), max(cy, h - cy)
    return (
        2 * np.degrees(theta_at(rx, fx, "horizontal")),
        2 * np.degrees(theta_at(ry, fy, "vertical")),
        2 * np.degrees(theta_at(float(np.hypot(rx, ry)), (fx + fy) / 2, "diagonal")),
    )


# ──────────────────────────────────────────────────────────────────────────────
# calibration.json
# ──────────────────────────────────────────────────────────────────────────────

def build_calibration_json(calib, maps: RectMaps, logger: logging.Logger,
                           refinement: "ExtrinsicRefinement | None" = None) -> CalibrationJson:
    # NOTE for consumers of the RECTIFIED video: the rectified cameras are the
    # raw ones rotated by R1 / R2, so the cam-IMU extrinsics move with them.
    # Those rotations and the resulting T_camrect_imu are published below rather
    # than left to be recomputed -- two independent harnesses recomputed them
    # and both got cam1 wrong by the refinement delta (1.11 deg / 13 px).
    """Standard Package v2 calibration.json for one pair.

    The imu block is passed through from the raw file untouched, including
    cam_imu_time_offset_s -- this adapter does NOT pre-apply the offset to the
    timestamps, so each downstream consumer applies it itself (same contract as
    the akai adapter).
    """
    w, h = calib.width, calib.height

    def raw_cam(K: np.ndarray, D: np.ndarray) -> CameraCalibration:
        # Non-strict here: a corner the KB4 polynomial cannot reach yields NaN,
        # which is an honest "unknown". Publishing the clamped 180.00 deg
        # instead would look like a measurement and be believed.
        hh, vv, dd = source_fov_deg(K, D, w, h, strict=False)
        if any(np.isnan(v) for v in (hh, vv, dd)):
            logger.warning(
                "raw FOV is not representable by the KB4 model at the image edge; "
                "publishing null rather than a clamped value"
            )
        # NaN is not valid JSON, and json.dumps would emit a bare `NaN` token
        # that strict parsers reject. Unknown is published as null.
        hh, vv, dd = (None if np.isnan(v) else float(v) for v in (hh, vv, dd))
        return CameraCalibration(
            fx=float(K[0, 0]), fy=float(K[1, 1]),
            cx=float(K[0, 2]), cy=float(K[1, 2]),
            h_fov_degrees=hh, v_fov_degrees=vv, d_fov_degrees=dd,
            distortion=[float(v) for v in np.asarray(D).ravel()],
            distortion_model="equidistant" if calib.distortion_model == "equidistant" else "radtan",
        )

    def rect_cam(P: np.ndarray) -> CameraCalibration:
        fx, fy = float(P[0, 0]), float(P[1, 1])
        # pinhole_fov, not the simpler focal-length-only formula: OpenCV's
        # camera is not centred on the frame, and this holds either way.
        hh, vv, dd = pinhole_fov(P, w, h)
        return CameraCalibration(
            fx=fx, fy=fy, cx=float(P[0, 2]), cy=float(P[1, 2]),
            h_fov_degrees=hh, v_fov_degrees=vv, d_fov_degrees=dd,
            distortion=[0.0, 0.0, 0.0, 0.0], distortion_model="radtan",
        )

    baseline_raw = float(np.linalg.norm(calib.T))
    raw_section = CalibrationSection(
        left=raw_cam(calib.K1, calib.D1),
        right=raw_cam(calib.K2, calib.D2),
        stereo=StereoInfo(baseline_mm=baseline_raw * 1000.0, baseline_meters=baseline_raw),
        rotation=[[float(v) for v in row] for row in np.asarray(calib.R)],
        translation=[float(v) for v in np.asarray(calib.T).ravel()],
    )
    rect_section = CalibrationSection(
        left=rect_cam(maps.P1),
        right=rect_cam(maps.P2),
        stereo=StereoInfo(baseline_mm=maps.baseline_m * 1000.0, baseline_meters=maps.baseline_m),
        rotation=[[1.0, 0.0, 0.0], [0.0, 1.0, 0.0], [0.0, 0.0, 1.0]],
        # Negative, matching the schema's stated convention (transforms LEFT
        # points into the RIGHT frame, so the right camera sits at -x in it),
        # the `raw` section above (T_x = -0.1057) and akai's rectified section.
        translation=[-maps.baseline_m, 0.0, 0.0],
    )

    src = calib.raw_data
    ex, imu_blk = src.get("extrinsics", {}), src.get("imu", {})
    temporal = src.get("temporal", {})
    imu_cal = ImuCalibration(
        T_cam0_imu=ex.get("T_cam0_imu", np.eye(4).tolist()),
        T_cam1_imu=ex.get("T_cam1_imu", np.eye(4).tolist()),
        accelerometer_noise_density=float(imu_blk.get("accelerometer_noise_density", 0.0)),
        accelerometer_random_walk=float(imu_blk.get("accelerometer_random_walk", 0.0)),
        gyroscope_noise_density=float(imu_blk.get("gyroscope_noise_density", 0.0)),
        gyroscope_random_walk=float(imu_blk.get("gyroscope_random_walk", 0.0)),
        # Both key spellings carry the same Kalibr timeshift (t_imu =
        # t_camera + shift): pre-2026-08 files say cam_imu_time_offset_s,
        # the 2026-08 vendor files say timeshift_cam_imu_s. Missing BOTH
        # would silently zero a real ~26 ms offset, hence the dual lookup.
        cam_imu_time_offset_s=float(
            temporal.get("cam_imu_time_offset_s",
                         temporal.get("timeshift_cam_imu_s", 0.0))),
    )
    # `raw` above carries the VENDOR rotation/translation untouched. When a
    # refinement was applied, what actually produced the rectified videos is
    # vendor_R composed with the delta -- recorded here rather than substituted
    # above, so the vendor calibration is never silently rewritten.
    ref_block = None
    if refinement is not None:
        ref_block = {k: v for k, v in vars(refinement).items()}
        if refinement.applied and refinement.refined_rotation is not None:
            # T_cam1_imu was DERIVED by the vendor as T_cam1_cam0 @ T_cam0_imu.
            # Refinement changes T_cam1_cam0, so the vendor's T_cam1_imu no longer
            # matches the geometry the rectified video was built from -- it stays
            # consistent with the UNREFINED calibration while the images do not.
            # Measured discrepancy on this device: 0.58 deg (eye), 1.12 deg
            # (front) ~= 13 px. Republished here rather than overwritten in the
            # imu block, so the vendor value survives untouched.
            R_ref = np.asarray(refinement.refined_rotation, dtype=np.float64)
            T_ref = np.eye(4)
            T_ref[:3, :3] = R_ref
            T_ref[:3, 3] = np.asarray(calib.T, dtype=np.float64).reshape(3)
            T_cam1_imu = T_ref @ np.asarray(imu_cal.T_cam0_imu, dtype=np.float64)
            ref_block["T_cam1_imu_refined"] = [[float(v) for v in row] for row in T_cam1_imu]
            ref_block["T_cam1_imu_refined_note"] = (
                "Use THIS for cam1 when consuming the rectified video; the imu "
                "block's T_cam1_imu corresponds to the unrefined vendor extrinsics."
            )

    # Everything a consumer needs to use the RECTIFIED pair with a VIO solver,
    # so nobody has to rebuild the rectification (OpenCV's camera or the
    # Bouguet fallback) to find it.
    T_cam0_imu = np.asarray(imu_cal.T_cam0_imu, dtype=np.float64)
    T_rect0_imu = np.eye(4)
    T_rect0_imu[:3, :3] = maps.R1
    T_rect0_imu = T_rect0_imu @ T_cam0_imu
    # A rectified pair is identity + baseline by construction, so cam1 follows
    # from cam0 -- never by rotating the vendor T_cam1_imu.
    T_shift = np.eye(4)
    T_shift[0, 3] = -maps.baseline_m
    rect_section_extra = {
        "R1_raw_to_rectified_left": [[float(v) for v in r] for r in maps.R1],
        "R2_raw_to_rectified_right": [[float(v) for v in r] for r in maps.R2],
        "T_cam0rect_imu": [[float(v) for v in r] for r in T_rect0_imu],
        "T_cam1rect_imu": [[float(v) for v in r] for r in (T_shift @ T_rect0_imu)],
        "convention": (
            "p_camXrect = T_camXrect_imu * p_imu, matching the imu block's "
            "T_cam0_imu convention. OKVIS-style solvers want the INVERSE of "
            "these (T_SC = camera->IMU); cuVSLAM-style solvers want them as-is "
            "(rig_from_imu with cam0 as the rig origin)."
        ),
    }

    return CalibrationJson(
        source="bitrobot_calibration", raw=raw_section, rectified=rect_section,
        imu=imu_cal, extrinsic_refinement=ref_block,
        rectified_extrinsics=rect_section_extra,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Encoding
# ──────────────────────────────────────────────────────────────────────────────

def remux(src: Path, dst: Path, frames: int | None = None) -> None:
    """Copy the video stream into a fresh mp4 without re-encoding.

    The source is already H.265 1080p; transcoding it would add generation loss
    for no benefit, so the raw trio is a stream copy.

    `frames` truncates to exactly that many frames. The v2 contract is that
    frame i of left_raw, frame i of right_raw and row i of frame_timestamps.csv
    are the same capture -- so when the two eyes disagree in length (this rig
    ships pairs off by one) every output must be cut to the shorter, not just
    the timeline.
    """
    limit = ["-frames:v", str(frames)] if frames else []
    subprocess.run(
        ["ffmpeg", "-v", "error", "-y", "-i", str(src), "-c", "copy",
         "-fps_mode", "passthrough", *limit, "-movflags", "+faststart", str(dst)],
        check=True,
    )


def _encoder_args(
    config: NormalizationConfig,
    source_bitrate_bps: int | None = None,
    streams: int = 1,
) -> list[str]:
    """ffmpeg quality flags for the configured encoder.

    Two things this has to get right.

    (1) Each family spells constant-quality differently: x264/x265 use -crf,
        NVENC uses -cq, VideoToolbox uses -q:v on an inverted 1-100 scale where
        HIGHER is better. Handing -crf to a hardware encoder is silently
        ignored and you get whatever bitrate the driver felt like.

    (2) The input is ALREADY compressed (~4 Mbit/s H.265 per eye). A quality
        target like CRF 20 then spends its bits faithfully reproducing the
        source's own compression artifacts: measured 30 Mbit/s for a 1080p SBS
        built from two 4 Mbit/s eyes -- a 2.2 GB file per 600 s chunk, ~4x the
        size of the footage it was derived from. So the rate is capped relative
        to the source: `source_bitrate_bps * streams * HEADROOM`. Quality-
        targeted encoders keep their CRF and get the cap as a ceiling
        (-maxrate/-bufsize); rate-targeted hardware encoders take it directly.
    """
    codec = config.video_codec
    # Remap/hstack add some high-frequency content, so allow headroom over the
    # straight sum of the source streams rather than matching it exactly.
    cap = int(source_bitrate_bps * streams * 1.5) if source_bitrate_bps else None

    # 2.1 Recording Requirements, on EVERY encoded stream:
    #   "No B-frames per Foxglove documentation. Maximum GOP/keyframe interval
    #    of 30 frames at 30 fps"
    #
    # Neither was set, so the encoder defaults applied and every delivered
    # rectified video carried B-frames and a 250-frame GOP. Measured on a real
    # file: 669 B-frames in the first 900, I-frames at 0/250/500/750.
    #
    # `-bf 0` was already applied to the raw re-encode for exactly the reason
    # that matters here too: B-frames make decode order differ from display
    # order, so anything indexing by frame number -- MCAP playback, a
    # frame-exact cut, a `-c copy` trim -- can land on the wrong frame. A
    # 250-frame GOP also makes seeking to an arbitrary frame cost up to 250
    # decodes. The rectified streams are the ones actually delivered, so they
    # need it more, not less.
    #
    # Cost, measured on 900 real frames (hevc_nvenc, cq 28): size +26.5%,
    # PSNR -0.46 dB (47.37 -> 46.91), encode time slightly LOWER (no reorder).
    conform = ["-bf", "0", "-g", str(GOP_MAX_FRAMES)]

    if codec.endswith("_nvenc"):
        args = ["-c:v", codec, "-preset", "p4", "-cq", str(config.video_crf)]
        if cap:
            args += ["-maxrate", str(cap), "-bufsize", str(cap * 2)]
        # Without -no-scenecut NVENC inserts extra I-frames at scene changes.
        # Harmless for the GOP ceiling, but it spends bits we just paid for.
        return args + conform + ["-no-scenecut", "1", "-pix_fmt", "yuv420p"]

    if codec.endswith("_videotoolbox"):
        if cap:  # VideoToolbox honours -b:v far more predictably than -q:v
            return (["-c:v", codec, "-b:v", str(cap)] + conform
                    + ["-pix_fmt", "yuv420p"])
        q = int(round(max(1.0, min(100.0, 100.0 - config.video_crf * (99.0 / 51.0)))))
        return ["-c:v", codec, "-q:v", str(q)] + conform + ["-pix_fmt", "yuv420p"]

    args = ["-c:v", codec, "-preset", config.video_preset, "-crf", str(config.video_crf)]
    if cap:
        args += ["-maxrate", str(cap), "-bufsize", str(cap * 2)]
    return args + conform + ["-pix_fmt", "yuv420p"]


def encode_rectified_pair(
    left_src: Path, right_src: Path,
    maps: RectMaps,
    out_left: Path, out_right: Path, out_sbs: Path | None,
    width: int, height: int, fps: float,
    config: NormalizationConfig,
    logger: logging.Logger,
    scale_to: tuple[int, int] | None = None,
    source_bitrate_bps: int | None = None,
    frames: int | None = None,
) -> None:
    """Decode both eyes once, remap, and write left/right/SBS in a single pass.

    Decoding each source once and feeding three encoders from that one pass
    avoids decoding 1080p HEVC three times over.
    """
    caps = [cv2.VideoCapture(str(p)) for p in (left_src, right_src)]
    if not all(c.isOpened() for c in caps):
        raise RuntimeError(f"cannot open {left_src.name} / {right_src.name} for rectification")

    ow, oh = scale_to if scale_to else (width, height)

    # stderr goes to a FILE, not a pipe and not DEVNULL. A pipe would deadlock
    # (nothing drains it while we are busy writing frames); DEVNULL makes a
    # failed encode invisible, which is how a truncated mp4 could pass the
    # contract gate -- validate_package() only checks that files exist and are
    # non-empty.
    err_files = {}

    def spawn(path: Path, w: int, h: int) -> subprocess.Popen:
        err = open(path.with_suffix(".encode.log"), "w+")
        err_files[path] = err
        return subprocess.Popen(
            ["ffmpeg", "-v", "error", "-y",
             "-f", "rawvideo", "-pix_fmt", "bgr24", "-s", f"{w}x{h}",
             "-r", f"{fps:.6f}", "-i", "pipe:0",
             *_encoder_args(config, source_bitrate_bps, 2 if w > width else 1),
             "-movflags", "+faststart", str(path)],
            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=err,
        )

    # out_sbs=None skips the composite (aux pairs: 57% of the bytes, no reader).
    targets = [(out_left, ow, oh), (out_right, ow, oh)]
    if out_sbs is not None:
        targets.append((out_sbs, ow * 2, oh))
    procs = [spawn(path, w_, h_) for path, w_, h_ in targets]
    n = 0
    try:
        while True:
            if frames is not None and n >= frames:
                break  # same n as frame_timestamps.csv and the raw trio
            okL, fL = caps[0].read()
            okR, fR = caps[1].read()
            if not (okL and okR):
                break
            rL = cv2.remap(fL, maps.map1_left, maps.map2_left, cv2.INTER_CUBIC,
                           borderMode=cv2.BORDER_CONSTANT)
            rR = cv2.remap(fR, maps.map1_right, maps.map2_right, cv2.INTER_CUBIC,
                           borderMode=cv2.BORDER_CONSTANT)
            if scale_to:
                rL = cv2.resize(rL, (ow, oh), interpolation=cv2.INTER_AREA)
                rR = cv2.resize(rR, (ow, oh), interpolation=cv2.INTER_AREA)
            procs[0].stdin.write(rL.tobytes())
            procs[1].stdin.write(rR.tobytes())
            if out_sbs is not None:
                procs[2].stdin.write(np.hstack([rL, rR]).tobytes())
            n += 1
            if n % 1000 == 0:
                logger.info(f"  rectified {n} frames")
    finally:
        for c in caps:
            c.release()
        for p in procs:
            if p.stdin:
                try:
                    p.stdin.close()
                except BrokenPipeError:
                    pass  # encoder already died; the returncode check below reports it
        # A timeout matters in an unattended batch: without one a wedged encoder
        # hangs the job until the Batch-level timeout kills it with no diagnosis.
        for (path, _, _), p in zip(targets, procs):
            try:
                rc = p.wait(timeout=ENCODER_FLUSH_TIMEOUT_S)
            except subprocess.TimeoutExpired:
                p.kill(); p.wait()
                raise RuntimeError(
                    f"encoder for {path.name} did not exit within "
                    f"{ENCODER_FLUSH_TIMEOUT_S}s; killed"
                )
            if rc != 0:
                err = err_files.get(path)
                tail = ""
                if err:
                    err.seek(0)
                    tail = err.read()[-2000:]
                raise RuntimeError(f"encoder for {path.name} exited {rc}: {tail}")
        for err in err_files.values():
            err.close()
        for path, _, _ in targets:
            path.with_suffix(".encode.log").unlink(missing_ok=True)

    # M2: a short read from cv2 must not silently produce a rectified video with
    # fewer frames than left_raw and frame_timestamps.csv. validate_package()
    # never compares frame counts, so nothing downstream would catch it.
    if frames is not None and n != frames:
        raise RuntimeError(
            f"rectification produced {n} frame pairs, expected {frames} -- "
            f"left_rectified would disagree with left_raw and frame_timestamps.csv"
        )
    logger.info(f"rectified {n} frame pairs -> " + ", ".join(
        path.name for path, _, _ in targets))


def emit_aligned_streams(
    left_src: Path, right_src: Path,
    idx_left: np.ndarray, idx_right: np.ndarray,
    width: int, height: int, fps: float,
    config: NormalizationConfig, logger: logging.Logger,
    raw_left: Path | None = None, raw_right: Path | None = None,
    maps: RectMaps | None = None,
    rect_left: Path | None = None, rect_right: Path | None = None,
    rect_sbs: Path | None = None,
    scale_to: tuple[int, int] | None = None,
    source_bitrate_bps: int | None = None,
) -> int:
    """Decode both eyes ONCE and write the association's frames to every target.

    Used when the association is not a plain prefix of both streams -- i.e. when
    a leading frame must be cut (the one-frame capture race) or interior frames
    removed (a one-sided stall). `-c copy` cannot do either: this device codes a
    30-frame GOP (verified: sync samples at 1, 31, 61, ...), so only the TAIL is
    stream-copyable, and mp4 edit lists are honoured too inconsistently by
    cv2/ffmpeg readers to be a contract.

    So the raw pair is re-encoded for those packages, and the cost is paid once
    by piggybacking on the decode the rectification pass needs anyway. One
    generation at a visually-lossless CRF is immaterial to feature tracking; a
    stereo pair that is not simultaneous is not.
    """
    caps = [cv2.VideoCapture(str(p)) for p in (left_src, right_src)]
    if not all(c.isOpened() for c in caps):
        raise RuntimeError(f"cannot open {left_src.name} / {right_src.name}")
    ow, oh = scale_to if scale_to else (width, height)
    err_files: dict[Path, object] = {}

    def spawn(path: Path, w: int, h: int, is_sbs: bool, is_raw: bool = False) -> subprocess.Popen:
        err = open(path.with_suffix(".encode.log"), "w+")
        err_files[path] = err
        # raw is the VIO front end's actual input -> near-lossless, and no
        # source-bitrate cap (the cap exists to stop a CRF target from inflating
        # an already-compressed source; here fidelity is the point).
        # `-bf 0`: the source carries no B-frames (no ctts box in any of the
        # 17,979 files), and keeping the re-encoded raw reorder-free means a
        # downstream `-frames:v n -c copy` still cuts where it thinks it does.
        # -bf 0 and -g now come from _encoder_args for every stream, so the
        # raw path no longer appends its own.
        enc = (_encoder_args(replace(config, video_crf=RAW_REENCODE_CRF), None, 1)
               if is_raw else
               _encoder_args(config, source_bitrate_bps, 2 if is_sbs else 1))
        return subprocess.Popen(
            ["ffmpeg", "-v", "error", "-y",
             "-f", "rawvideo", "-pix_fmt", "bgr24", "-s", f"{w}x{h}",
             "-r", f"{fps:.6f}", "-i", "pipe:0", *enc,
             "-movflags", "+faststart", str(path)],
            stdin=subprocess.PIPE, stdout=subprocess.DEVNULL, stderr=err,
        )

    targets: list[tuple[Path, int, int, bool, bool]] = []
    for p in (raw_left, raw_right):
        if p is not None:
            targets.append((p, ow, oh, False, True))
    for p in (rect_left, rect_right):
        if p is not None:
            targets.append((p, ow, oh, False, False))
    if rect_sbs is not None:
        targets.append((rect_sbs, ow * 2, oh, True, False))
    procs = {p: spawn(p, w, h, sbs, raw) for p, w, h, sbs, raw in targets}

    written = 0
    pos = [-1, -1]           # last frame index read from each capture
    frames = {}
    try:
        for il, ir in zip(idx_left.tolist(), idx_right.tolist()):
            short = False
            for c, want, k in ((caps[0], il, 0), (caps[1], ir, 1)):
                while pos[k] < want:
                    ok, fr = c.read()
                    if not ok:
                        # The container's nb_frames routinely overcounts the
                        # DECODABLE frames by one on this recorder (measured:
                        # stsz says 18003, cv2 yields 18002 — a trailing packet
                        # that carries no frame). Refusing the chunk here threw
                        # away otherwise-perfect 600 s segments. Stop cleanly
                        # instead; the caller truncates every output and the
                        # timestamp file to what was actually emitted, so the
                        # row-i==frame-i invariant still holds.
                        short = True
                        break
                    pos[k] += 1
                if short:
                    break
                frames[k] = fr
            if short:
                logger.warning(
                    f"decode ended at source frame {pos} after {written} pairs "
                    f"(container promised more); truncating the package to {written}")
                break
            fL, fR = frames[0], frames[1]
            if raw_left is not None:
                a, b = fL, fR
                if scale_to:
                    a = cv2.resize(a, (ow, oh), interpolation=cv2.INTER_AREA)
                    b = cv2.resize(b, (ow, oh), interpolation=cv2.INTER_AREA)
                procs[raw_left].stdin.write(a.tobytes())
                procs[raw_right].stdin.write(b.tobytes())
            if maps is not None:
                rL = cv2.remap(fL, maps.map1_left, maps.map2_left, cv2.INTER_CUBIC,
                               borderMode=cv2.BORDER_CONSTANT)
                rR = cv2.remap(fR, maps.map1_right, maps.map2_right, cv2.INTER_CUBIC,
                               borderMode=cv2.BORDER_CONSTANT)
                if scale_to:
                    rL = cv2.resize(rL, (ow, oh), interpolation=cv2.INTER_AREA)
                    rR = cv2.resize(rR, (ow, oh), interpolation=cv2.INTER_AREA)
                if rect_left is not None:
                    procs[rect_left].stdin.write(rL.tobytes())
                    procs[rect_right].stdin.write(rR.tobytes())
                if rect_sbs is not None:
                    procs[rect_sbs].stdin.write(np.hstack([rL, rR]).tobytes())
            written += 1
            if written % 1000 == 0:
                logger.info(f"  aligned+encoded {written} frame pairs")
    finally:
        for c in caps:
            c.release()
        for p in procs.values():
            if p.stdin:
                try:
                    p.stdin.close()
                except BrokenPipeError:
                    pass
        for path, _, _, _, _ in targets:
            pr = procs[path]
            try:
                rc = pr.wait(timeout=ENCODER_FLUSH_TIMEOUT_S)
            except subprocess.TimeoutExpired:
                pr.kill(); pr.wait()
                raise RuntimeError(f"encoder for {path.name} did not exit within "
                                   f"{ENCODER_FLUSH_TIMEOUT_S}s; killed")
            if rc != 0:
                err = err_files.get(path)
                tail = ""
                if err:
                    err.seek(0); tail = err.read()[-2000:]
                raise RuntimeError(f"encoder for {path.name} exited {rc}: {tail}")
        for err in err_files.values():
            err.close()
        for path, _, _, _, _ in targets:
            path.with_suffix(".encode.log").unlink(missing_ok=True)

    if written == 0:
        raise RuntimeError(f"{left_src.name}/{right_src.name}: no frame pair could be decoded")
    logger.info(f"emitted {written} aligned frame pairs -> "
                + ", ".join(p.name for p, _, _, _, _ in targets))
    return written


def encode_sbs_raw(left: Path, right: Path, out: Path, config: NormalizationConfig,
                   source_bitrate_bps: int | None = None,
                   frames: int | None = None, fps: float = 30.0) -> None:
    """Side-by-side of the RAW pair. Unlike the raw singles this must be
    encoded, because hstack changes pixels.

    Both inputs are re-stamped onto a common index grid BEFORE hstack. This is
    not cosmetic. The two eyes carry independently jittered PTS, and hstack's
    framesync emits one output frame per timestamp in the UNION of its inputs --
    roughly 2x the frames, each pairing left frame i with right frame i-1.
    Truncating that to `frames` then yields a file that looks the right length
    but covers only ~54% of the recording (measured: 324.2 s of a 598.7 s chunk)
    with every frame duplicated. `-fps_mode passthrough` does not prevent this --
    it is what exposes it. Re-stamping each input by frame index forces the
    pairing, which is what sbs_rectified gets for free by being assembled
    frame-by-frame in Python.

    The SBS is a derived convenience view, so putting it on a uniform grid costs
    nothing: frame_timestamps.csv remains the authoritative per-frame timing.
    """
    limit = ["-frames:v", str(frames)] if frames else []
    fc = (
        f"[0:v]setpts=N/({fps:.6f}*TB)[a];"
        f"[1:v]setpts=N/({fps:.6f}*TB)[b];"
        f"[a][b]hstack=inputs=2[v]"
    )
    subprocess.run(
        ["ffmpeg", "-v", "error", "-y", "-i", str(left), "-i", str(right),
         "-filter_complex", fc, "-map", "[v]",
         "-fps_mode", "cfr", "-r", f"{fps:.6f}", *limit,
         *_encoder_args(config, source_bitrate_bps, streams=2),
         "-movflags", "+faststart", str(out)],
        check=True,
    )



def _processing_metrics(timer: ExecutionTimer) -> ProcessingMetrics:
    """ProcessingMetrics from the repo's ExecutionTimer snapshot."""
    m = timer.get_metrics()
    return ProcessingMetrics(
        duration_seconds=m.duration_seconds,
        compute_type=m.compute_type,
        memory_used_mb=m.cpu.memory_used_mb if m.cpu else None,
        memory_percent=m.cpu.memory_percent if m.cpu else None,
        gpu_name=m.gpu.gpu_name if m.gpu else None,
        gpu_memory_used_mb=m.gpu.gpu_memory_used_mb if m.gpu else None,
    )


# ──────────────────────────────────────────────────────────────────────────────
# Rectification QC (rectification_qc.json)
# ──────────────────────────────────────────────────────────────────────────────
#
# Runs on the WRITTEN package once every video is encoded, and decodes the
# delivered files, so it judges what downstream receives, codec and all. Every
# rectified video (the stereo pairs and the far mono streams) gets three
# checks, in this order:
#
#   border      remap fill must cover at most QC_BORDER_REJECT_PCT of the frame
#   distortion  the shipped calibration's lens model must be physical over the
#               angles the remap samples, and re-rectifying the raw frame from
#               that calibration must reproduce the delivered frame
#   epipolar    stereo pairs only: vertical disparity of matched features.
#               Measured and recorded, never judged.
#
# A video passes when border and distortion both pass; a check that could not
# be measured fails closed. The chunk verdict is left_rectified.mp4's and
# right_rectified.mp4's alone: the eye and far streams get the same checks and
# a verdict of their own, recorded after the core pair's with the epipolar
# statistics, and never judged. Ported from the border-detection QC (border_qc:
# detector.analyse_border, calibration.lens_model_check, remap.analyse_remap
# and distortion_verdict, epipolar.analyse_epipolar) with its thresholds, so
# the numbers are directly comparable with that tool's.

QC_SCHEMA = "bitrobot-rectification-qc"
QC_SCHEMA_VERSION = 1
# Fractions of the frame COUNT: frames are addressed by index (see _qc_frames).
QC_SAMPLE_FRACTIONS = (0.08, 0.28, 0.50, 0.72, 0.92)
QC_DECODE_TIMEOUT_S = 120
# Border. A pixel at or below QC_DARK_THRESHOLD on every channel of every
# sampled frame is a candidate; an edge-touching candidate region counts as fill
# only when QC_FILL_CORE_MIN_FRACTION of it is at or below QC_FILL_CORE_THRESHOLD.
# Dim scenery clusters in 9..16; fill is ~0 inside a thin compression rim.
QC_DARK_THRESHOLD = 16
QC_FILL_CORE_THRESHOLD = 8
QC_FILL_CORE_MIN_FRACTION = 0.85
QC_BORDER_REJECT_PCT = 1.5
# Distortion. Faithful output measured 0.00 px; one frame of motion is 2-12 px.
QC_REMAP_TOLERANCE_PX = 3.0
QC_REMAP_MIN_MATCHES = 30
# Epipolar, statistics only.
QC_ORB_FEATURES = 4000
QC_LOWE_RATIO = 0.75
QC_INLIER_MAD_K = 3.0
QC_MIN_CORRESPONDENCES = 25
QC_COVERAGE_GRID = 4
MAD_TO_SIGMA = 1.4826        # MAD -> sigma for a normal distribution
HUBER_DELTA_MULT = 1.345     # canonical Huber tuning, 95% Gaussian efficiency


def _qc_video(path: Path) -> tuple[np.ndarray, int, int]:
    """(presentation times, width, height) of one package video."""
    stream = _ffprobe_json(path, ["-select_streams", "v:0",
                                  "-show_entries", "stream=width,height"])["streams"][0]
    return frame_pts_seconds(path), int(stream["width"]), int(stream["height"])


def _qc_frames(path: Path, pts: np.ndarray, indices: list[int],
               width: int, height: int) -> dict[int, np.ndarray]:
    """BGR frames at exact frame indices; a frame that does not decode is absent.

    Seeks to the midpoint between frame i-1 and frame i, never to frame i's own
    time: the raw streams are stream copies whose PTS jitter by ~0.2 ms, so a
    seek ON a frame time lands a frame early or late at random, and the raw and
    rectified frames compared are then different captures (measured: a fake
    ~30 px remap mismatch). -seek_timestamp puts -ss on the same absolute clock
    as the container PTS.
    """
    size = width * height * 3
    frames: dict[int, np.ndarray] = {}
    for i in indices:
        t = float(pts[0]) if i == 0 else float(pts[i - 1] + pts[i]) / 2.0
        try:
            res = subprocess.run(
                ["ffmpeg", "-nostdin", "-v", "error", "-seek_timestamp", "1",
                 "-ss", f"{t:.6f}", "-i", str(path), "-frames:v", "1",
                 "-f", "rawvideo", "-pix_fmt", "bgr24", "-"],
                capture_output=True, timeout=QC_DECODE_TIMEOUT_S, check=False)
        except subprocess.TimeoutExpired:
            continue
        if res.returncode == 0 and len(res.stdout) >= size:
            frames[i] = np.frombuffer(res.stdout[:size], np.uint8).reshape(height, width, 3)
    return frames


def qc_border(planes: list[np.ndarray]) -> tuple[dict, np.ndarray | None]:
    """Remap fill in max-over-BGR planes: (result, fill mask).

    A region is fill only when it passes all three tests: dark in EVERY sampled
    frame (fill cannot move), connected to a frame edge (fill is anchored to
    one), and mostly near-absolute black (a dim wall or a vignetted corner is
    dark, but not ~0).
    """
    if len(planes) < 2:
        return {"status": "not_measured", "frames": len(planes),
                "reason": f"{len(planes)} frame(s) decoded; the static test needs 2"}, None
    h, w = planes[0].shape
    static_dark = np.logical_and.reduce([p <= QC_DARK_THRESHOLD for p in planes])
    static_core = np.logical_and.reduce([p <= QC_FILL_CORE_THRESHOLD for p in planes])
    border = np.zeros((h, w), dtype=bool)
    candidate_px = core_px = rejected_px = interior_px = candidates = rejected = 0
    if static_dark.any():
        n_labels, labels, stats, _ = cv2.connectedComponentsWithStats(
            static_dark.view(np.uint8), connectivity=8)
        for label in range(1, n_labels):
            x, y, cw, ch, area = (int(v) for v in stats[label])
            if not (x == 0 or y == 0 or x + cw >= w or y + ch >= h):
                interior_px += area
                continue
            window = slice(y, y + ch), slice(x, x + cw)
            component = labels[window] == label
            core = int((component & static_core[window]).sum())
            candidate_px += area
            candidates += 1
            core_px += core
            if core >= QC_FILL_CORE_MIN_FRACTION * area:
                border[window] |= component
            else:
                rejected_px += area
                rejected += 1
    border_px = int(border.sum())
    border_pct = 100.0 * border_px / (w * h)
    edges = {"top": int(border[0].sum()), "bottom": int(border[-1].sum()),
             "left": int(border[:, 0].sum()), "right": int(border[:, -1].sum())}
    return {
        "status": "pass" if border_pct <= QC_BORDER_REJECT_PCT else "fail",
        "border_pct": border_pct,
        "border_px": border_px,
        "candidate_border_pct": 100.0 * candidate_px / (w * h),
        "fill_core_frac": core_px / candidate_px if candidate_px else 0.0,
        "rejected_edge_dark_px": rejected_px,
        "candidate_components": candidates,
        "rejected_components": rejected,
        "interior_dark_px": interior_px,
        "exact_zero_frac": (float(((planes[0] == 0) & border).sum()) / border_px
                            if border_px else 0.0),
        "edges": "|".join(name[0].upper() for name, px in edges.items() if px),
        "edge_px": edges,
        "frames": len(planes),
        # A seek can hand back one frame twice; the static test is vacuous then.
        "distinct_frames": len({hash(p.tobytes()) for p in planes}),
    }, border


def _qc_lens(D: np.ndarray, model: str, P: np.ndarray, R: np.ndarray,
             w: int, h: int) -> dict:
    """Is the lens model physical over the angles this remap samples?"""
    used = max_sampled_angle_deg(P, R, w, h)
    if model != "equidistant":
        return {"model": model, "max_sampled_angle_deg": used, "valid": None,
                "reason": f"lens check covers the equidistant model, not {model}"}
    fold, ok, reason = lens_validity(D, used)
    return {"model": model, "max_sampled_angle_deg": used, "fold_deg": fold,
            "valid": ok, "reason": reason}


def _qc_offsets(candidate: np.ndarray, delivered: np.ndarray) -> np.ndarray | None:
    """Displacement of every cross-checked ORB match between two frames."""
    orb = cv2.ORB_create(nfeatures=QC_ORB_FEATURES)
    kp_a, des_a = orb.detectAndCompute(candidate, None)
    kp_b, des_b = orb.detectAndCompute(delivered, None)
    if des_a is None or des_b is None:
        return None
    matches = [m for m in cv2.BFMatcher(cv2.NORM_HAMMING, crossCheck=True).match(des_a, des_b)
               if m.distance < 40]
    if len(matches) < QC_REMAP_MIN_MATCHES:
        return None
    a = np.array([kp_a[m.queryIdx].pt for m in matches])
    b = np.array([kp_b[m.trainIdx].pt for m in matches])
    return np.hypot(a[:, 0] - b[:, 0], a[:, 1] - b[:, 1])


def qc_remap(raw_gray: list[np.ndarray], rect_gray: list[np.ndarray],
             maps: tuple[np.ndarray, np.ndarray]) -> dict:
    """Re-rectify raw frames from the shipped calibration; compare with delivery.

    The lists hold the same frame indices of the two videos, and frame i of a
    raw and of its rectified video are one capture by the package contract.
    """
    medians, p90s, counts = [], [], []
    for raw, delivered in zip(raw_gray, rect_gray):
        candidate = cv2.remap(raw, maps[0], maps[1], cv2.INTER_LINEAR,
                              borderMode=cv2.BORDER_CONSTANT, borderValue=0)
        shift = _qc_offsets(candidate, delivered)
        if shift is not None:
            medians.append(float(np.median(shift)))
            p90s.append(float(np.percentile(shift, 90)))
            counts.append(len(shift))
    if not p90s:
        return {"status": "unmeasured", "frames": 0}
    # Decided on the 90th percentile, not the median: a wrong lens model mostly
    # moves the periphery, where few matched features sit (a wrong model scored
    # 1.7 px at the median and 12.4 px at p90).
    p90 = float(np.median(p90s))
    return {"status": "consistent" if p90 <= QC_REMAP_TOLERANCE_PX else "inconsistent",
            "offset_px": float(np.median(medians)), "offset_p90_px": p90,
            "matches": int(np.median(counts)), "frames": len(p90s)}


def qc_distortion(lens: dict, remap: dict) -> dict:
    """Pass when the lens model is physical and the video matches its calibration."""
    if lens.get("valid") is False:
        status, verdict, reason = "fail", "calibration_invalid", lens["reason"]
    elif remap.get("status") == "inconsistent":
        status, verdict = "fail", "remap_mismatch"
        reason = (f"delivered frame differs from its own calibration by "
                  f"{remap['offset_p90_px']:.1f} px (p90)")
    elif lens.get("valid") is True:
        status, verdict = "pass", "rectified_ok"
        reason = "calibration physically valid over the field used"
    else:
        status, verdict = "not_measured", "unverified"
        reason = lens.get("reason") or "no usable calibration"
    return {"status": status, "verdict": verdict, "reason": reason,
            "lens": lens, "remap": remap}


def _qc_correspondences(gray_l: np.ndarray, gray_r: np.ndarray, orb, matcher,
                        mask_l: np.ndarray | None, mask_r: np.ndarray | None) -> np.ndarray:
    """(uL, vL, uR, vR) of Lowe-ratio matches kept only where L->R and R->L agree.

    Detection skips each eye's fill: it has no texture, but its boundary throws
    strong spurious corners. No fundamental-matrix RANSAC: the pair is meant to
    be rectified, and dy ~ 0 is the stronger prior.
    """
    kp_l, des_l = orb.detectAndCompute(gray_l, None if mask_l is None else (~mask_l).view(np.uint8))
    kp_r, des_r = orb.detectAndCompute(gray_r, None if mask_r is None else (~mask_r).view(np.uint8))
    if des_l is None or des_r is None or len(kp_l) < 3 or len(kp_r) < 3:
        return np.empty((0, 4))

    def ratio_map(src: np.ndarray, dst: np.ndarray) -> dict[int, int]:
        best: dict[int, int] = {}
        for m in matcher.knnMatch(src, dst, k=2):
            if len(m) == 2 and m[0].distance < QC_LOWE_RATIO * m[1].distance:
                best[m[0].queryIdx] = m[0].trainIdx
        return best

    forward, backward = ratio_map(des_l, des_r), ratio_map(des_r, des_l)
    points = [[*kp_l[li].pt, *kp_r[ri].pt] for li, ri in forward.items()
              if backward.get(ri) == li]
    return np.array(points) if len(points) >= 3 else np.empty((0, 4))


def _qc_inliers(dy: np.ndarray) -> np.ndarray:
    """Inliers at median +/- k * scale, with a scale that cannot collapse to zero.

    Rectified dy is discrete and often more than half exactly zero, so a plain
    MAD is 0 and would keep every gross mismatch: escalate p50 -> p75 -> p90 of
    |dy - median| to the first non-zero value.
    """
    deviation = np.abs(dy - np.median(dy))
    for quantile in (50, 75, 90):
        scale = float(np.percentile(deviation, quantile)) * MAD_TO_SIGMA
        if scale > 0:
            return deviation <= QC_INLIER_MAD_K * scale
    return deviation == 0


def _qc_drift_plane(u: np.ndarray, v: np.ndarray,
                    dy: np.ndarray) -> tuple[np.ndarray, float, float, float]:
    """Huber-IRLS fit of dy = a*u + b*v + c: (coef, r2, systematic_rms, residual_rms).

    c is a constant vertical offset, a a relative roll, b a vertical scale
    mismatch; r2 near 0 means the residual is random, near 1 a coherent plane.
    """
    design = np.column_stack([u, v, np.ones_like(u)])
    coef, *_ = np.linalg.lstsq(design, dy, rcond=None)
    for _ in range(5):
        residual = dy - design @ coef
        scale = float(np.median(np.abs(residual - np.median(residual)))) * MAD_TO_SIGMA
        if scale == 0:
            break
        delta = HUBER_DELTA_MULT * scale
        weights = np.ones_like(dy)
        far = np.abs(residual) > delta
        weights[far] = delta / np.abs(residual[far])
        coef, *_ = np.linalg.lstsq(design * weights[:, None], dy * weights, rcond=None)
    fitted = design @ coef
    residual = dy - fitted
    ss_tot = float(np.sum((dy - dy.mean()) ** 2))
    r2 = max(0.0, 1.0 - float(np.sum(residual ** 2)) / ss_tot) if ss_tot > 0 else 0.0
    return coef, r2, float(np.sqrt(np.mean(fitted ** 2))), float(np.sqrt(np.mean(residual ** 2)))


def qc_epipolar(gray_l: list[np.ndarray], gray_r: list[np.ndarray],
                mask_l: np.ndarray | None, mask_r: np.ndarray | None) -> dict:
    """Vertical-disparity statistics of a rectified pair over the same frames."""
    frames = min(len(gray_l), len(gray_r))
    if frames < 1:
        return {"measurement_status": "not_measurable", "frames": 0}
    orb = cv2.ORB_create(nfeatures=QC_ORB_FEATURES)
    matcher = cv2.BFMatcher(cv2.NORM_HAMMING)
    blocks = [b for b in (_qc_correspondences(gl, gr, orb, matcher, mask_l, mask_r)
                          for gl, gr in zip(gray_l, gray_r)) if len(b)]
    row: dict = {"frames": frames, "matches": int(sum(len(b) for b in blocks))}
    if row["matches"] < QC_MIN_CORRESPONDENCES:
        return {"measurement_status": "insufficient_matches", **row}
    points = np.vstack(blocks)
    dy, dx = points[:, 1] - points[:, 3], points[:, 0] - points[:, 2]
    keep = _qc_inliers(dy)
    row.update(inliers=int(keep.sum()), inlier_ratio=float(keep.mean()))
    if row["inliers"] < QC_MIN_CORRESPONDENCES:
        return {"measurement_status": "insufficient_matches", **row}
    dy_in, dx_in = dy[keep], dx[keep]
    coef, r2, systematic_rms, residual_rms = _qc_drift_plane(
        points[keep, 0], points[keep, 1], dy_in)
    h, w = gray_l[0].shape[:2]
    g = QC_COVERAGE_GRID
    cells = {(min(int(y / h * g), g - 1), min(int(x / w * g), g - 1))
             for x, y in points[keep, :2]}
    return {
        "measurement_status": "ok", **row,
        "vertical_disparity_median": float(np.median(np.abs(dy_in))),
        "vertical_disparity_rms": float(np.sqrt(np.mean(dy_in ** 2))),
        "vertical_disparity_p95": float(np.percentile(np.abs(dy_in), 95)),
        "vertical_disparity_max": float(np.abs(dy_in).max()),
        "horizontal_disparity_median": float(np.median(dx_in)),
        "disparity_positive_frac": float((dx_in > 0).mean()),
        "coverage": len(cells) / float(g * g),
        "drift_coef_u": float(coef[0]), "drift_coef_v": float(coef[1]),
        "drift_offset_px": float(coef[2]), "drift_r2": r2,
        "drift_systematic_rms": systematic_rms, "drift_residual_rms": residual_rms,
    }


def _qc_stereo_geometry(calib: dict, eye: str) -> tuple:
    """(K, D, model, R, P) of one eye, from the package calibration.json."""
    raw, rect = calib["raw"][eye], calib["rectified"][eye]
    rotation = calib["rectified_extrinsics"][
        "R1_raw_to_rectified_left" if eye == "left" else "R2_raw_to_rectified_right"]
    return (np.array([[raw["fx"], 0, raw["cx"]], [0, raw["fy"], raw["cy"]], [0, 0, 1.0]]),
            np.asarray(raw["distortion"], dtype=np.float64), raw["distortion_model"],
            np.asarray(rotation, dtype=np.float64),
            np.array([[rect["fx"], 0, rect["cx"]], [0, rect["fy"], rect["cy"]], [0, 0, 1.0]]))


def _qc_mono_geometry(doc: dict) -> tuple:
    """(K, D, model, R, P) of a mono stream, from its package calibration file."""
    cam = next(iter(doc["vendor"]["cameras"].values()))
    fx, fy, cx, cy = cam["intrinsics"]
    rect = doc["rectified"]
    return (np.array([[fx, 0, cx], [0, fy, cy], [0, 0, 1.0]]),
            np.asarray(cam["distortion_coefficients"], dtype=np.float64),
            cam.get("distortion_model", "equidistant"), np.eye(3),
            np.array([[rect["fx"], 0, rect["cx"]], [0, rect["fy"], rect["cy"]], [0, 0, 1.0]]))


def _qc_measure(out: Path, group: list[tuple[str, str, tuple]]) -> list[tuple]:
    """Border and distortion of each (rectified, raw, geometry) in `group`.

    Returns (results, fill mask, luma planes by frame index) per video. One
    index set serves the whole group, so a stereo pair's eyes, and each video's
    raw, are sampled on identical captures.
    """
    info = {name: _qc_video(out / name) for rect, raw, _ in group for name in (rect, raw)}
    n = min(len(pts) for pts, _, _ in info.values())
    if n < 2:
        raise ValueError(f"the shortest video has {n} frame(s)")
    indices = sorted({round(f * (n - 1)) for f in QC_SAMPLE_FRACTIONS})
    measured = []
    for rect, raw, (K, D, model, R, P) in group:
        pts, w, h = info[rect]
        frames = _qc_frames(out / rect, pts, indices, w, h)
        gray = {i: cv2.cvtColor(f, cv2.COLOR_BGR2GRAY) for i, f in frames.items()}
        border, mask = qc_border([frames[i].max(axis=2) for i in sorted(frames)])
        del frames
        raw_pts, raw_w, raw_h = info[raw]
        if (raw_w, raw_h) != (w, h):
            remap = {"status": "unmeasured", "reason": f"raw is {raw_w}x{raw_h}, rectified {w}x{h}"}
        else:
            raw_gray = {i: cv2.cvtColor(f, cv2.COLOR_BGR2GRAY)
                        for i, f in _qc_frames(out / raw, raw_pts, indices, w, h).items()}
            common = [i for i in sorted(gray) if i in raw_gray]
            if model == "equidistant":
                maps = cv2.fisheye.initUndistortRectifyMap(
                    K, D[:4].reshape(4, 1), R, P, (w, h), cv2.CV_32FC1)
            else:
                maps = cv2.initUndistortRectifyMap(K, D, R, P, (w, h), cv2.CV_32FC1)
            remap = qc_remap([raw_gray[i] for i in common], [gray[i] for i in common], maps)
        measured.append(({"sampled_frame_indices": sorted(gray), "border": border,
                          "distortion": qc_distortion(_qc_lens(D, model, P, R, w, h), remap)},
                         mask, gray))
    return measured


def _qc_verdict(entry: dict) -> None:
    """pass iff border and distortion both pass; epipolar never enters."""
    if "error" in entry:
        entry["verdict"], entry["reason"] = "fail", f"QC could not run: {entry['error']}"
        return
    border, distortion = entry["border"], entry["distortion"]
    failed = []
    if border["status"] != "pass":
        failed.append(f"border {border['border_pct']:.2f}% exceeds {QC_BORDER_REJECT_PCT:g}%"
                      if border["status"] == "fail"
                      else f"border not measured ({border['reason']})")
    if distortion["status"] != "pass":
        failed.append(f"distortion {distortion['verdict']}: {distortion['reason']}")
    entry["verdict"] = "fail" if failed else "pass"
    entry["reason"] = "; ".join(failed) or (
        f"border {border['border_pct']:.2f}% <= {QC_BORDER_REJECT_PCT:g}%, "
        f"distortion {distortion['verdict']}")


def rectification_qc(out: Path, pair_reports: dict, logger: logging.Logger) -> dict:
    """Border, distortion and epipolar QC of every rectified video in the package.

    The chunk verdict is the core pair's alone (left_rectified.mp4 and
    right_rectified.mp4). Every other stream is measured the same way and, with
    the epipolar statistics, recorded after it without entering the verdict.
    """
    started = time.monotonic()
    videos: list[dict] = []
    epipolar: dict[str, dict] = {}
    not_checked: dict[str, str] = {}

    def entry(video: str, stream: str, pair: str | None, eye: str, raw: str,
              calibration: str) -> dict:
        return {"video": video, "stream": stream, "stereo_pair": pair, "eye": eye,
                "raw": raw, "calibration": calibration, "verdict": "fail", "reason": ""}

    for pair_name, *_ in PAIRS:
        rep = pair_reports.get(pair_name)
        if rep is None:
            continue
        a = rep.get("artifacts") or {}
        if not (rep.get("rectified") and a.get("calibration")):
            not_checked[pair_name] = str(rep.get("error") or "no rectified video (raw-only pair)")
            continue
        eyes = {eye: entry(a[f"rectified_{eye}"], f"{pair_name}_{eye}", pair_name, eye,
                           a[f"raw_{eye}"], a["calibration"]) for eye in ("left", "right")}
        try:
            calib = json.loads((out / a["calibration"]).read_text())
            (res_l, mask_l, gray_l), (res_r, mask_r, gray_r) = _qc_measure(out, [
                (e["video"], e["raw"], _qc_stereo_geometry(calib, eye))
                for eye, e in eyes.items()])
            eyes["left"].update(res_l)
            eyes["right"].update(res_r)
            common = sorted(set(gray_l) & set(gray_r))
            epipolar[pair_name] = {
                "used_for_verdict": False,
                "left": eyes["left"]["video"], "right": eyes["right"]["video"],
                **qc_epipolar([gray_l[i] for i in common], [gray_r[i] for i in common],
                              mask_l, mask_r),
            }
        except Exception as exc:  # noqa: BLE001 -- one pair's QC must not lose the others
            for e in eyes.values():
                e["error"] = f"{type(exc).__name__}: {exc}"
        videos.extend(eyes.values())

    for stem, _, role in MONO_STREAMS:
        rep = pair_reports.get(stem)
        if rep is None:
            continue
        a = rep.get("artifacts") or {}
        if not (a.get("rectified") and a.get("calibration")):
            not_checked[stem] = str(rep.get("error") or "no rectified video (no calibration)")
            continue
        e = entry(a["rectified"], role, None, stem.split("_")[0], a["raw"], a["calibration"])
        try:
            doc = json.loads((out / a["calibration"]).read_text())
            [(res, _, _)] = _qc_measure(out, [(e["video"], e["raw"], _qc_mono_geometry(doc))])
            e.update(res)
        except Exception as exc:  # noqa: BLE001 -- one stream's QC must not lose the others
            e["error"] = f"{type(exc).__name__}: {exc}"
        videos.append(e)

    for e in videos:
        _qc_verdict(e)
        (logger.info if e["verdict"] == "pass" else logger.warning)(
            f"rectification QC {e['video']}: {e['verdict']} -- {e['reason']}")
    main = [e for e in videos if e["stereo_pair"] == "core"]
    failed = [e for e in main if e["verdict"] != "pass"]
    if len(main) == 2 and not failed:
        verdict, reason = "pass", f"{main[0]['video']} and {main[1]['video']} pass border and distortion"
    else:
        verdict = "fail"
        reason = "; ".join(f"{e['video']}: {e['reason']}" for e in failed) or (
            f"core pair not checked: {not_checked.get('core', 'no rectified video')}")
    (logger.info if verdict == "pass" else logger.warning)(
        f"rectification QC verdict: {verdict} -- {reason}")
    # The verdict and the two videos it is decided on come first; everything
    # after main_videos is recorded for diagnosis and never judged.
    return {
        "schema": QC_SCHEMA,
        "schema_version": QC_SCHEMA_VERSION,
        "verdict": verdict,
        "reason": reason,
        "verdict_rule": (
            "the chunk passes when left_rectified.mp4 and right_rectified.mp4 each pass "
            "border and distortion; a check that could not be measured fails. The eye and "
            "far streams get the same checks and a verdict of their own, and the stereo "
            "pairs' epipolar statistics are recorded; neither enters the chunk verdict."),
        "main_videos": main,
        "other_videos": [e for e in videos if e["stereo_pair"] != "core"],
        "epipolar": epipolar,
        # How each stereo pair's maps were built: the method (OpenCV or the
        # Bouguet fallback, and why), the cover zoom, per-eye field and lens.
        "rectification": {name: rep["rectification_diagnostics"] for name, rep in pair_reports.items()
                          if isinstance(rep, dict) and "rectification_diagnostics" in rep},
        "not_checked": not_checked,
        "thresholds": {
            "rectified_min_fov_deg": list(RECTIFY_SPEC_MIN_FOV_DEG),
            "border_reject_pct": QC_BORDER_REJECT_PCT,
            "dark_threshold": QC_DARK_THRESHOLD,
            "fill_core_threshold": QC_FILL_CORE_THRESHOLD,
            "fill_core_min_fraction": QC_FILL_CORE_MIN_FRACTION,
            "remap_tolerance_px": QC_REMAP_TOLERANCE_PX,
            "lens_physical_tolerance": LENS_PHYSICAL_TOLERANCE,
            "lens_fisheye_family_tolerance": LENS_FISHEYE_FAMILY_TOLERANCE,
        },
        "sample_frame_fractions": list(QC_SAMPLE_FRACTIONS),
        "elapsed_s": round(time.monotonic() - started, 1),
    }


def write_rectification_qc(out: Path, pair_reports: dict, logger: logging.Logger) -> dict:
    """Run the QC and write rectification_qc.json beside chunk_report.json.

    Never raises: the QC judges the package and must not be able to lose it. A
    fault in the QC itself is written into the file as a failed result.
    """
    try:
        report = rectification_qc(out, pair_reports, logger)
    except Exception as exc:  # the QC must never fail the chunk
        logger.exception("rectification QC failed")
        report = {"schema": QC_SCHEMA, "schema_version": QC_SCHEMA_VERSION, "verdict": "fail",
                  "reason": f"QC could not run: {type(exc).__name__}: {exc}"}
    try:
        (out / "rectification_qc.json").write_text(json.dumps(report, indent=2))
    except OSError as exc:
        logger.error(f"cannot write rectification_qc.json: {exc}")
    return report


# ──────────────────────────────────────────────────────────────────────────────
# Adapter
# ──────────────────────────────────────────────────────────────────────────────

@register_adapter
class BitrobotAdapter(DeviceAdapter):
    """Normalizes a RoboCap chunk into one Standard Package v2 per stereo pair."""

    device_name = "bitrobot"

    @property
    def required_files(self) -> list[str]:
        # imu_*.db matches both layouts' calibrated IMU (imu_right_XXX.db
        # pre-2026-08, imu_XXX.db after); find_chunk picks the right one.
        return ["calibration.json", "validation.json",
                "left_*.mp4", "right_*.mp4", "imu_*.db"]

    def validate_output(self, output_dir: Path) -> list[str]:
        """Core Standard Package v2 gate PLUS the bitrobot aux invariants.

        The eye pair fills the core contract, so validate_package() runs
        unchanged. On top of it, per pair (core AND aux): every video's frame
        count must equal its timestamp file's row count — the one rule the v2
        gate does not have, and which a real defect (a side-by-side carrying
        the right frame count over half the recording) already exploited.
        Every file declared in meta.aux_streams must be present, non-empty
        and internally consistent.
        """
        errors = list(validate_package(output_dir))

        def frame_count(v: Path) -> int | None:
            try:
                return int(subprocess.run(
                    ["ffprobe", "-v", "error", "-select_streams", "v:0",
                     "-show_entries", "stream=nb_frames", "-of", "csv=p=0",
                     str(v)],
                    check=True, capture_output=True, text=True,
                ).stdout.strip())
            except (subprocess.CalledProcessError, ValueError):
                errors.append(f"{v.name}: unreadable frame count")
                return None

        def ts_rows(path: Path) -> list[float] | None:
            try:
                rows = [ln for ln in path.read_text().splitlines()[1:] if ln.strip()]
                vals = [float(r.split(",")[1]) for r in rows]
            except (OSError, ValueError, IndexError) as exc:
                errors.append(f"{path.name}: unreadable ({exc})")
                return None
            if not vals:
                errors.append(f"{path.name}: zero data rows")
                return None
            if any(b < a for a, b in zip(vals, vals[1:])):
                errors.append(f"{path.name}: not monotonic")
            return vals

        # Core pair: frame i == row i for EVERY video of the pair.
        core_ts = ts_rows(output_dir / "frame_timestamps.csv")
        if core_ts is not None:
            for v in ("left_raw.mp4", "right_raw.mp4", "sbs_raw.mp4",
                      "left_rectified.mp4", "right_rectified.mp4",
                      "sbs_rectified.mp4"):
                path = output_dir / v
                if not path.exists():
                    continue  # absence is validate_package's finding
                n = frame_count(path)
                if n is not None and n != len(core_ts):
                    errors.append(
                        f"{v}: {n} frames but frame_timestamps.csv has "
                        f"{len(core_ts)} rows")

        # Stereo timing is a CONTRACT, not a log line. Before this, every
        # mistiming finding was a logger.warning that nobody reads in an
        # unattended Batch run, and a package whose two eyes were captured a
        # frame apart reached OKVIS looking perfectly well-formed -- the one
        # check that could have caught it (okvis's timestamp_tolerance) is
        # blinded because build_clip.py writes the left timeline into both
        # cam0/data.csv and cam1/data.csv.
        try:
            rep = json.loads((output_dir / "chunk_report.json").read_text())
        except (OSError, ValueError):
            rep = {}
        for pair_name, r in (rep.get("pairs") or {}).items():
            if not isinstance(r, dict) or "residual_skew_ms" not in r:
                continue
            resid = r["residual_skew_ms"]
            if resid > PAIR_MATCH_TOLERANCE_S * 1e3:
                errors.append(
                    f"pair '{pair_name}': residual left/right capture skew {resid:.3f} ms "
                    f"exceeds {PAIR_MATCH_TOLERANCE_S * 1e3:.1f} ms -- the emitted frames are "
                    f"not simultaneous and must not be rectified as a stereo pair")
            if r.get("coverage") is not None and r["coverage"] < MIN_PAIR_COVERAGE:
                errors.append(
                    f"pair '{pair_name}': stereo coverage {r['coverage'] * 100:.1f}% is below "
                    f"{MIN_PAIR_COVERAGE * 100:.0f}%")

        # Aux streams, exactly as declared by meta.aux_streams.
        try:
            aux = (json.loads((output_dir / "meta.json").read_text())
                   .get("aux_streams") or {})
        except (OSError, ValueError):
            aux = {}
        checked_ts: dict[str, list[float] | None] = {}
        for v in aux.get("videos") or []:
            ts_name = v.get("frame_timestamps")
            if ts_name and ts_name not in checked_ts:
                checked_ts[ts_name] = ts_rows(output_dir / ts_name)
            vals = checked_ts.get(ts_name)
            for key in ("raw", "rectified"):
                fname = v.get(key)
                if not fname:
                    continue
                path = output_dir / fname
                if not path.exists() or path.stat().st_size == 0:
                    errors.append(f"aux file missing or empty: {fname}")
                    continue
                n = frame_count(path)
                if vals is not None and n is not None and n != len(vals):
                    errors.append(
                        f"{fname}: {n} frames but {ts_name} has {len(vals)} rows")
            cal = v.get("calibration")
            if cal and (not (output_dir / cal).exists()
                        or (output_dir / cal).stat().st_size == 0):
                errors.append(f"aux calibration missing or empty: {cal}")
        for s in aux.get("sensors") or []:
            path = output_dir / s
            if not path.exists() or path.stat().st_size == 0:
                errors.append(f"aux sensor missing or empty: {s}")
        return errors

    @property
    def optional_files(self) -> list[str]:
        return ["calibration_*.json", "*_eye_*.mp4", "*_front_*.mp4",
                "*_far_*.mp4", "imu_left_*.db", "mag_middle_*.db"]

    def normalize(
        self,
        session_dir: Path,
        output_dir: Path,
        config: NormalizationConfig,
    ) -> NormalizationResult:
        logger = self.get_logger()
        timer = ExecutionTimer(
            compute_type="gpu" if config.video_codec.endswith("_nvenc") else "cpu"
        )
        timer.__enter__()

        chunk, errors = find_chunk(session_dir, logger)
        if chunk is None:
            return NormalizationResult(success=False, error="; ".join(errors) or "no chunk found")
        for e in errors:
            logger.warning(e)

        calib_raw = json.loads((session_dir / "calibration.json").read_text())
        accel_lsb, gyro_lsb = read_raw_sensor_scales(session_dir / "calibration.json", logger)

        try:
            validation_raw = json.loads((session_dir / "validation.json").read_text())
        except (OSError, ValueError) as exc:
            return NormalizationResult(
                success=False,
                error=f"validation.json unreadable: {exc}. Calibration-solve QA "
                      f"is required for bitrobot; this chunk is not publishable.")
        cal_validation_report = calibration_validation.evaluate(
            validation_raw, thresholds=config.calibration_validation_thresholds)
        output_dir.mkdir(parents=True, exist_ok=True)
        (output_dir / "calibration_validation_report.json").write_text(
            json.dumps(cal_validation_report, indent=2))
        (output_dir / "calibration_validation_verdict.json").write_text(json.dumps({
            "device_id": cal_validation_report["device_id"],
            "status": cal_validation_report["status"],
            "failed_checks": cal_validation_report["failed_checks"],
        }, indent=2))
        if cal_validation_report["status"] == "fail":
            # Fail before IMU load, rectification, extrinsic refinement, or
            # encoding -- not after, so a bad calibration solve doesn't pay
            # for compute it can never publish.
            return NormalizationResult(
                success=False,
                error="calibration_validation: "
                      + "; ".join(cal_validation_report["failed_checks"])
                      + " out of threshold")

        # The primary IMU load was OUTSIDE any try, so a corrupt sqlite (8 of
        # 6,011 in the corpus) escaped as an unhandled DatabaseError and killed
        # the Batch job with no diagnosis instead of failing the chunk cleanly.
        try:
            imu = load_imu(
                chunk.imu_calibrated, logger,
                accel_lsb_per_g=accel_lsb, gyro_lsb_per_dps=gyro_lsb,
                expected_rate_hz=float(calib_raw.get("imu", {}).get("update_rate_hz") or 0) or None,
            )
        except StreamUnreadable as exc:
            return NormalizationResult(
                success=False,
                error=f"calibrated IMU unusable [{exc.code}]: {exc}. imu.csv is in the core "
                      f"v2 contract and DROID degrades silently to an unanchored vision-only "
                      f"solve without it, so this chunk is not publishable.")
        except ValueError as exc:
            return NormalizationResult(success=False, error=f"calibrated IMU rejected: {exc}")

        # ONE shared rebase origin for every pair in the chunk: the earliest
        # instant any sensor produced data. Keeps imu.csv and every pair's
        # frame_timestamps.csv on one timeline, so the pairs stay comparable to
        # each other as well as to the IMU.
        # Probed defensively: this runs OUTSIDE the per-pair try, so letting one
        # unreadable video raise here would defeat the pair isolation below and
        # lose eye+front because far's tag was corrupt.
        video_starts: dict[str, float] = {}
        stream_status: dict[str, str] = {}
        nominal_dt = 1.0 / 30.0
        for stem, vp in chunk.videos.items():
            try:
                vi = probe_video(vp)
                video_starts[stem] = vi.start_device_s
                stream_status[stem] = "OK"
                if vi.fps > 1:
                    nominal_dt = 1.0 / vi.fps
            except StreamUnreadable as exc:
                stream_status[stem] = exc.code
                logger.error(f"cannot use {vp.name}: {exc}")
            except Exception as exc:
                stream_status[stem] = "UNREADABLE"
                logger.error(f"cannot read start time from {vp.name}: {exc}")
        for _, lstem, rstem, _ in PAIRS:
            for stem in (lstem, rstem):
                stream_status.setdefault(stem, "ABSENT")
        for stem, _, _ in MONO_STREAMS:
            stream_status.setdefault(stem, "ABSENT")
        if not video_starts:
            return NormalizationResult(
                success=False,
                error="no video carries a readable `comment` start tag; "
                      "frame<->IMU sync is impossible",
            )

        # ── Segment coherence, BEFORE the origin is chosen ───────────────────
        # 44 segments in the corpus hold two recordings ~3.5 h apart under
        # identical filenames. Beyond the pairing damage, `origin` is a min()
        # over every video start, so one stale aux tag drags the CORE package's
        # timeline hours backwards. This has to gate before that min().
        coherence = segment_coherence(video_starts, nominal_dt)
        # The CORE (eye) pair decides whether the chunk exists at all. An aux
        # camera with a wild start tag is a stream problem, not a segment
        # problem: measured on the corpus, a single stale aux stream is the
        # usual shape (e.g. left_front 329.5 s adrift while the other five agree
        # to a frame), and failing the whole chunk there threw away a perfectly
        # good eye pair. Only a split that reaches the eye pair is fatal.
        core = {k: v for k, v in video_starts.items() if k in ("left", "right")}
        core_coh = segment_coherence(core, nominal_dt) if len(core) == 2 else None
        if core_coh is not None and not core_coh["coherent"]:
            return NormalizationResult(
                success=False,
                error=(
                    f"the core (eye) pair is not one recording: left/right start tags differ "
                    f"by {core_coh['spread_s']:.1f}s ({core_coh['spread_frames']:.0f} frame "
                    f"periods, threshold {SEGMENT_COHERENCE_FRAMES:.0f}). Two recordings were "
                    f"uploaded under the same robocap_segment<N>_* filenames, so these "
                    f"streams are not a rig capture and must not be paired."),
            )
        # Quarantine streams that do not belong to the core recording.
        #
        # ANCHOR ON THE IMU, not on a camera. The IMU carries no `comment` tag —
        # its first sample IS the device clock, so it cannot be stale relative to
        # itself. Anchoring on the earliest eye tag (what this did before) is
        # backwards: measured across the 44 split segments, the IMU t0 sits with
        # the LATER cluster in 31 of them and with the LARGEST cluster in 38, so
        # the stale tag is usually the EARLIER one. Anchoring on it kept the
        # stale stream and quarantined the good ones.
        # Cluster the CAMERAS among themselves, then let the IMU pick which
        # cluster is genuine. Two different distances are involved and they must
        # not share a threshold:
        #   camera <-> camera : max legitimate spread 0.0334 s (one frame) —
        #                       anything past 10 frame periods is a collision.
        #   camera <-> IMU    : the IMU legitimately opens up to ~1.25 s BEFORE
        #                       the cameras (the "IMU leads" pattern, ~19% of
        #                       segments), and ~32 ms after them in most of the
        #                       rest.
        # Applying the camera-to-camera threshold to the camera-to-IMU distance
        # rejects perfectly healthy segments — measured on two of these 29, where
        # all six cameras agreed to 0.0001 s but the IMU led by 1.2 s.
        incoherent: set[str] = set()
        ref = imu.stats["raw_t_start_s"]
        if video_starts:
            ordered = sorted(video_starts.items(), key=lambda kv: kv[1])
            clusters: list[list[tuple[str, float]]] = [[ordered[0]]]
            for name, t0 in ordered[1:]:
                if t0 - clusters[-1][-1][1] > SEGMENT_COHERENCE_FRAMES * nominal_dt:
                    clusters.append([])
                clusters[-1].append((name, t0))
            if len(clusters) > 1:
                # the IMU cannot be stale relative to itself: whichever camera
                # cluster sits nearest its first sample is the real recording.
                best = min(clusters,
                           key=lambda c: min(abs(t - ref) for _, t in c))
                for c in clusters:
                    if c is best:
                        continue
                    for name, _ in c:
                        incoherent.add(name)
                        stream_status[name] = "INCOHERENT_START"
        if incoherent:
            logger.warning(
                f"quarantining stream(s) {sorted(incoherent)}: start tag more than "
                f"{SEGMENT_COHERENCE_FRAMES:.0f} frame periods from the eye pair "
                f"(segment spread {coherence['spread_s']:.1f}s). The core package is "
                f"unaffected; pairs using these streams are skipped.")
        coherence["quarantined"] = sorted(incoherent)
        if coherence["warn"] and not incoherent:
            logger.warning(
                f"camera start tags span {coherence['spread_s'] * 1e3:.1f} ms "
                f"({coherence['spread_frames']:.1f} frame periods)")
        # The second IMU, on the SAME origin so the two are directly comparable.
        # Supplementary: a failure here must not lose the chunk, and it is never
        # offered to the contract gate.
        secondary = None
        if chunk.imu_secondary is not None:
            try:
                secondary = load_imu(
                    chunk.imu_secondary, logger,
                    accel_lsb_per_g=accel_lsb, gyro_lsb_per_dps=gyro_lsb,
                    expected_imuid=SECONDARY_IMUID,
                )
            except Exception as exc:
                logger.warning(
                    f"secondary IMU {chunk.imu_secondary.name} unusable, skipping: {exc}"
                )

        # ONE shared rebase origin for every pair in the chunk, built only from
        # quantities a consumer can re-derive from the source files: the first RAW
        # accelerometer timestamp and the mp4 comment tags. Using the post-trim
        # grid head here instead shifts the origin by ~15 ms -- 0.45 frames, and
        # 75% of the calibrated cam_imu_time_offset_s -- with the shift varying
        # per chunk.
        # Built from a FIXED, mandatory set -- the raw IMU head and the two eye
        # cameras -- not from whichever streams happened to probe successfully.
        # With min() over all six, two runs of the same chunk with a different
        # subset readable produced DIFFERENT timelines for every file in the
        # package, which defeats the point of publishing the origin at all. The
        # eye pair is required for the chunk to succeed, so it is always there.
        origin_inputs = {"imu_raw_start": imu.stats["raw_t_start_s"]}
        for stem in ("left", "right"):
            if stem in video_starts and stem not in incoherent:
                origin_inputs[stem] = video_starts[stem]
        origin = min(origin_inputs.values())
        samples = imu_samples(imu, origin)

        # ONE package per chunk: the core pair (plain left/right) fills the
        # core v2 contract at the package root; every other pair rides along
        # as an aux stream (suffixed names, declared in meta.aux_streams).
        # The CORE pair is mandatory — without it there is no core package
        # for downstream to read.
        eye_meta: MetaJson | None = None
        pair_reports: dict[str, dict] = {}
        for pair_name, lstem, rstem, calib_file in PAIRS:
            if lstem not in chunk.videos or rstem not in chunk.videos:
                if pair_name == "core":
                    logger.warning(f"pair '{pair_name}': videos missing, skipping")
                continue
            if lstem in incoherent or rstem in incoherent:
                bad = sorted({lstem, rstem} & incoherent)
                logger.warning(
                    f"pair '{pair_name}': skipping, {bad} came from a different recording")
                pair_reports[pair_name] = {
                    "error": f"stream(s) {bad} have a start tag from a different recording"}
                continue
            try:
                meta_obj, report = self._build_pair(
                    pair_name, chunk.videos[lstem], chunk.videos[rstem],
                    _resolve_pair_calibration(pair_name, calib_file,
                                              chunk.calibrations),
                    chunk, imu, origin,
                    output_dir, config, logger, timer,
                    is_primary=(pair_name == "core"),
                )
                pair_reports[pair_name] = report
                if meta_obj is not None:
                    eye_meta = meta_obj
            except Exception as exc:  # one bad aux pair must not lose the chunk
                logger.exception(f"pair '{pair_name}' failed: {exc}")
                pair_reports[pair_name] = {"error": str(exc)}

        # Independent mono streams — no association, no rectification; each
        # publishes its raw video, its own timeline and its own calibration.
        for stem, calib_file, _role in MONO_STREAMS:
            if stem not in chunk.videos:
                continue
            if stem in incoherent:
                logger.warning(
                    f"mono stream '{stem}': skipping, start tag from a different recording")
                pair_reports[stem] = {
                    "error": "start tag from a different recording"}
                continue
            try:
                pair_reports[stem] = self._build_mono_stream(
                    stem, chunk.videos[stem], chunk.calibrations.get(calib_file),
                    imu, origin, output_dir, config, logger)
            except Exception as exc:  # one bad mono stream must not lose the chunk
                logger.exception(f"mono stream '{stem}' failed: {exc}")
                pair_reports[stem] = {"error": str(exc)}

        if eye_meta is None or "error" in pair_reports.get("core", {"error": "missing"}):
            return NormalizationResult(
                success=False,
                error="core pair (left/right) did not normalize: "
                      + str(pair_reports.get("core", {}).get("error", "videos missing")),
            )
        if pair_reports["core"].get("calibration_source") is None and \
                pair_reports["core"]["artifacts"]["calibration"] is None:
            return NormalizationResult(
                success=False,
                error="core pair produced no calibration.json — the core v2 "
                      "contract cannot be satisfied",
            )

        write_imu_csv(samples, output_dir / "imu.csv")

        # Rebase provenance, once per chunk. imu.csv and the timestamp files
        # are written to 6 decimals, so a consumer that needs to get back onto
        # the device clock (or line this chunk up with its neighbours) is
        # handed the origin at full precision instead of inferring it.
        (output_dir / "sync.json").write_text(json.dumps({
            "clock": "free-running monotonic device counter, not wall clock",
            "t_origin_device_s": origin,
            "imu_first_sample_device_s": imu.stats["raw_t_start_s"],
            "imu_head_trim_s": imu.stats["head_trim_s"],
            "video_start_device_s": video_starts,
            "video_comment_tag_units": "microseconds",
            "cam_imu_time_offset_s_NOT_applied": True,
            # Reproducibility: the origin is a min() over exactly these, never
            # over whichever streams happened to be readable.
            "t_origin_inputs": origin_inputs,
            "segment_coherence": coherence,
            "stream_status": stream_status,
            "imu_gaps": imu.stats.get("gaps", []),
            "imu_gyro_interp_rows_suppressed": imu.stats.get(
                "gyro_interp_rows_suppressed", 0),
            "imu_coverage_device_s": [float(imu.t_device_s[0]), float(imu.t_device_s[-1])],
            "accel_saturation_ms2": imu.stats.get("accel_saturation_ms2"),
            "gyro_saturation_rads": imu.stats.get("gyro_saturation_rads"),
        }, indent=2))

        aux_sensors: list[str] = []
        if secondary is not None:
            write_imu_csv(imu_samples(secondary, origin), output_dir / "imu_secondary.csv")
            aux_sensors.append("imu_secondary.csv")
            (output_dir / "imu_secondary.README").write_text(
                "imu_secondary.csv -- the device's SECOND 6-DoF IMU "
                f"(imuid_={secondary.stats['imuid']}, {secondary.stats['rate_hz']:.2f} Hz).\n\n"
                "NOT CALIBRATED. No shipped calibration file describes it: there is no\n"
                "T_cam_imu and no noise model for this sensor, so it CANNOT be fused with\n"
                "the cameras as-is. Do not substitute it for imu.csv.\n\n"
                "It is published because it is real data the normalized package would\n"
                "otherwise drop, and it is on the SAME rebased clock as imu.csv and\n"
                "frame_timestamps.csv, so the two IMUs are directly comparable.\n\n"
                "Geometry, measured from this recording (NOT from a calibration file):\n"
                "  rotation to the primary  ~179.5 deg   (Kabsch fit on the gyro triads,\n"
                "                                         residual 0.036 rad/s)\n"
                "  separation               ~117 mm      (lever-arm fit)\n"
                "calibration.json's _provenance quotes ~179 deg / ~129 mm for the same\n"
                "pair, which is independent agreement -- but neither is a substitute for\n"
                "a proper Kalibr imu1 calibration. Ask the vendor for one before using\n"
                "this stream in any solve.\n"
            )
            logger.info(
                f"secondary IMU published: {secondary.stats['n_output']} samples @ "
                f"{secondary.stats['rate_hz']:.2f} Hz (uncalibrated)"
            )

        # aux_streams: the authoritative inventory of everything beyond the
        # core contract, consumed by chunking (which cuts every declared file
        # into every chunk) and ignored by every other stage.
        aux_videos: list[dict] = []
        for pair_name, *_ in PAIRS:
            if pair_name == "core":
                continue
            rep = pair_reports.get(pair_name)
            if not rep or "artifacts" not in rep:
                continue
            a = rep["artifacts"]
            for side in ("left", "right"):
                aux_videos.append({
                    "name": f"{side}_{pair_name}",
                    "raw": a[f"raw_{side}"],
                    "rectified": a[f"rectified_{side}"],
                    "frame_timestamps": a["frame_timestamps"],
                    "calibration": a["calibration"],
                    "role": f"{pair_name}_{side}",
                })
        for stem, _, role in MONO_STREAMS:
            rep = pair_reports.get(stem)
            if not rep or "artifacts" not in rep:
                continue
            a = rep["artifacts"]
            aux_videos.append({
                "name": stem,
                "raw": a["raw"],
                # per-camera UNDISTORTION (pinhole), not stereo rectification
                "rectified": a["rectified"],
                "frame_timestamps": a["frame_timestamps"],
                "calibration": a["calibration"],
                "role": role,
            })
        if aux_videos or aux_sensors:
            eye_meta.aux_streams = {"videos": aux_videos, "sensors": aux_sensors}
        core_rep = dict(pair_reports.get("core") or {})
        core_rep.setdefault("pair", "core")
        eye_meta.grade = build_grade(core_rep, imu.stats, coherence,
                                     sorted(incoherent), nominal_dt)
        logger.info(f"delivery grade: tier={eye_meta.grade['tier']} "
                    f"vio={eye_meta.grade['vio']} reasons={eye_meta.grade['reasons']}")
        eye_meta.save(output_dir / "meta.json")

        timer.__exit__(None, None, None)
        # chunk_report.json keeps its schema: the rectification diagnostics are
        # published in rectification_qc.json, not here.
        (output_dir / "chunk_report.json").write_text(json.dumps({
            "chunk": chunk.chunk_num,
            "device": imu.metadata,
            "imu": imu.stats,
            "imu_secondary": secondary.stats if secondary else None,
            "rebase_origin_device_s": origin,
            "video_start_device_s": video_starts,
            "pairs": {name: {k: v for k, v in rep.items() if k != "rectification_diagnostics"}
                      for name, rep in pair_reports.items()},
        }, indent=2))

        # Border, distortion and epipolar QC of every rectified video, read back
        # from the written files. Never fatal (see write_rectification_qc).
        write_rectification_qc(output_dir, pair_reports, logger)

        # One package at output_dir: the entrypoint gates it through
        # validate_output(), which runs the core v2 gate PLUS the aux-stream
        # invariants declared in meta.aux_streams.
        return NormalizationResult(
            success=True,
            package=StandardPackage(path=output_dir, meta=eye_meta),
            packages=[output_dir],
        )

    def _build_mono_stream(
        self, stem: str, src: Path, calib_path: Path | None,
        imu: BitrobotImu, origin: float, out: Path,
        config: NormalizationConfig, logger: logging.Logger,
    ) -> dict:
        """One independent (non-stereo) camera, published as an aux stream.

        Raw video stream-copied whole, one frame-timestamps file on the shared
        chunk clock, and — when the per-camera calibration is present — an
        UNDISTORTED video ({stem}_rectified.mp4: fisheye -> ideal pinhole,
        same idea as the mono pipeline's video_rectified; NOT stereo
        rectification, there is no second camera sharing this view). The
        package calibration file wraps the vendor document verbatim and adds
        the undistorted view's pinhole intrinsics. Deliberately NO
        association and NO IMU-coverage trimming: the stream is not a VIO
        input — any future consumer aligns by time through the shared clock.
        """
        logger.info(f"=== mono stream '{stem}' ===")
        out.mkdir(parents=True, exist_ok=True)
        vi = probe_video(src)
        pts = frame_pts_seconds(src, vi.frame_count)
        n = int(len(pts))
        if n < 2:
            raise ValueError(f"mono stream '{stem}': no decodable frame timing")
        t_rebased = (vi.start_device_s + pts) - origin

        raw_path = out / f"{stem}_raw.mp4"
        # -frames:v n only ever trims the container's overcount-by-one tail
        # (n == the packet count), so this stays a pure stream copy.
        remux(src, raw_path, n)
        ts_path = out / f"frame_timestamps_{stem}.csv"
        write_frame_timestamps_csv([float(v) for v in t_rebased[:n]], ts_path)

        calib_name = rect_name = None
        new_K = None
        if calib_path is not None:
            cal = load_mono_calibration(calib_path)
            if (cal["width"], cal["height"]) != (vi.width, vi.height):
                raise ValueError(
                    f"mono stream '{stem}': {calib_path.name} is calibrated for "
                    f"{cal['width']}x{cal['height']} but the video is "
                    f"{vi.width}x{vi.height}")
            m1, m2, new_K = undistort_maps_mono(cal)
            rect_path = out / f"{stem}_rectified.mp4"
            # Decode the already-cut raw copy so the undistorted stream has
            # exactly the timestamps file's n frames by construction.
            undistort_stream(
                raw_path, rect_path, m1, m2, vi.width, vi.height, vi.fps, n,
                config, vi.bitrate_bps, logger)
            rect_name = rect_path.name
            calib_name = calib_path.name
            (out / calib_name).write_text(json.dumps({
                "schema": "bitrobot-mono-stream-calibration",
                "schema_version": 1,
                "stream": stem,
                # The vendor document, VERBATIM — never rewritten.
                "vendor": cal["doc"],
                # What {stem}_rectified.mp4 was actually built with: an ideal
                # pinhole (zero distortion) at balance/alpha 0.
                "rectified": {
                    "fx": float(new_K[0, 0]), "fy": float(new_K[1, 1]),
                    "cx": float(new_K[0, 2]), "cy": float(new_K[1, 2]),
                    "width": vi.width, "height": vi.height,
                    "distortion": [0.0, 0.0, 0.0, 0.0],
                    "distortion_model": "pinhole",
                    "undistorted_from_model": cal["model"],
                    "balance": 0.0,
                },
            }, indent=2))
        else:
            logger.warning(
                f"mono stream '{stem}': no per-camera calibration in the upload "
                f"-- raw footage published without one, no undistorted video")

        imu_hi = float(imu.t_device_s[-1])
        frames_past_imu = int(((vi.start_device_s + pts[:n]) > imu_hi).sum())
        logger.info(
            f"mono stream '{stem}': {n} frames stream-copied, "
            f"undistorted={'yes' if rect_name else 'no'}, "
            f"{frames_past_imu} frame(s) past the IMU end (kept)")
        return {
            "artifacts": {
                "raw": raw_path.name,
                "rectified": rect_name,
                "frame_timestamps": ts_path.name,
                "calibration": calib_name,
            },
            "source": src.name,
            "frames_written": n,
            "fps": vi.fps,
            "video_start_device_s": vi.start_device_s,
            "video_start_rebased_s": float(t_rebased[0]),
            "frames_past_imu_end": frames_past_imu,
            "mono_stream": True,
            "rectified": rect_name is not None,
            "undistorted_pinhole_fx": float(new_K[0, 0]) if new_K is not None else None,
        }

    def _build_pair(
        self, pair: str, left_src: Path, right_src: Path,
        calib_path: "Path | tuple[Path, Path] | None",
        chunk: BitrobotChunk, imu: BitrobotImu,
        origin: float, out: Path, config: NormalizationConfig,
        logger: logging.Logger, timer: ExecutionTimer,
        is_primary: bool = False,
    ) -> tuple[MetaJson | None, dict]:
        logger.info(f"=== pair '{pair}' ===")
        out.mkdir(parents=True, exist_ok=True)
        # The eye pair fills the core v2 names unprefixed; aux pairs get
        # suffixed names so all three coexist in one package directory.
        name = lambda f: out / _artifact_name(pair, f)

        vl, vr = probe_video(left_src), probe_video(right_src)
        if (vl.width, vl.height) != (vr.width, vr.height):
            raise ValueError(f"pair '{pair}': left/right geometry differs")

        # ── Associate by CAPTURE TIME, per docs/06 invariant 1 ───────────────
        # Frame timestamps are device-clock start + container PTS. The old code
        # paired by index (n = min(len_l, len_r)), which silently emitted stereo
        # pairs that were not simultaneous whenever the two eyes started a frame
        # apart (24% of eye pairs) or one of them stalled mid-clip.
        pts_l = frame_pts_seconds(left_src, vl.frame_count)
        pts_r = frame_pts_seconds(right_src, vr.frame_count)
        tl_dev = vl.start_device_s + pts_l
        tr_dev = vr.start_device_s + pts_r
        # fps from the PTS grid itself, not (n-1)/duration: the latter is only
        # right because this muxer writes a final sample duration of 0.
        dt = float(np.median(np.diff(pts_l))) if len(pts_l) > 1 else 1.0 / max(vl.fps, 1.0)
        start_skew_ms = float((vl.start_device_s - vr.start_device_s) * 1e3)

        assoc = associate_frames(tl_dev, tr_dev, dt)
        if len(assoc.idx_left) == 0:
            raise ValueError(
                f"pair '{pair}': no frame of {left_src.name} is simultaneous with any frame "
                f"of {right_src.name} within {PAIR_MATCH_TOLERANCE_S * 1e3:.1f} ms "
                f"(capture starts differ by {start_skew_ms / 1e3:.1f} s). These two videos are "
                f"not a stereo pair -- refusing to rectify them.")

        # ── Continuity: never let one package span a break in the trajectory ─
        a, b, dropped_runs = longest_contiguous_run(assoc.t_pair, VIDEO_GAP_SPLIT_S)
        if dropped_runs:
            kept_s = float(assoc.t_pair[b - 1] - assoc.t_pair[a])
            logger.warning(
                f"pair '{pair}': timeline breaks at gap(s) > {VIDEO_GAP_SPLIT_S}s; keeping the "
                f"longest run ({kept_s:.1f}s, frames {a}..{b - 1}) and discarding "
                f"{sum(r['frames'] for r in dropped_runs)} frame(s) in {len(dropped_runs)} "
                f"other run(s). A package spanning the hole would be two trajectories.")
        idx_l, idx_r = assoc.idx_left[a:b], assoc.idx_right[a:b]
        t_sel = assoc.t_pair[a:b]

        # ── Clamp to IMU coverage ────────────────────────────────────────────
        # The IMU log typically opens ~1 frame AFTER the first video frame, so
        # the leading frame(s) have no inertial data. OKVIS silently drops
        # uncovered frames, DROID-VI wrappers do not, and the row-i==frame-i
        # invariant means no downstream stage can drop them safely -- so do it
        # here. Costs 33 ms of footage in the common case.
        # KEEP the IMU-uncovered head; do not cut it.
        #
        # The IMU log opens a median ~32 ms AFTER the first video frame, so the
        # leading frame is uncovered on 80.7% of eye pairs. v1 dropped it, which
        # made the association a non-prefix and forced the raw pair through a
        # CRF-14 re-encode on ~81% of the corpus -- against this module's own
        # documented contract that the raw trio is STREAM-COPIED and never
        # transcoded, and at 1.45x-4.54x the bytes.
        #
        # Dropping it was only ever necessary because there was no channel to
        # SAY the head is uncovered. meta.grade now provides one: the covered
        # range is published, validate_output enforces it, and a VIO stage
        # starts at first_covered instead of frame 0. Trimming the tail is still
        # required -- a frame after the IMU ends can never be preintegrated to.
        imu_lo, imu_hi = imu.t_device_s[0], imu.t_device_s[-1]
        head_uncovered = int((t_sel < imu_lo).sum())
        tail_uncovered = int((t_sel > imu_hi).sum())
        n_uncovered = head_uncovered + tail_uncovered
        if tail_uncovered:
            keep = t_sel <= imu_hi
            logger.info(
                f"pair '{pair}': trimming {tail_uncovered} tail frame(s) past the IMU end "
                f"({(t_sel[-1] - imu_hi) * 1e3:.1f} ms)")
            idx_l, idx_r, t_sel = idx_l[keep], idx_r[keep], t_sel[keep]
        if head_uncovered:
            logger.info(
                f"pair '{pair}': {head_uncovered} leading frame(s) precede the first IMU "
                f"sample by {(imu_lo - t_sel[0]) * 1e3:.1f} ms -- KEPT and published as "
                f"grade.timing.imu_covered_frame_range; VIO must start at frame "
                f"{head_uncovered}")

        n = int(len(idx_l))
        span_s = float(t_sel[-1] - t_sel[0]) if n > 1 else 0.0
        if n < 2 or span_s < MIN_RETAINED_SPAN_S:
            raise ValueError(
                f"pair '{pair}': only {n} simultaneous frame(s) spanning {span_s:.2f}s survive "
                f"association, continuity and IMU coverage (minimum {MIN_RETAINED_SPAN_S}s)")
        if assoc.coverage < MIN_PAIR_COVERAGE:
            raise ValueError(
                f"pair '{pair}': only {assoc.coverage * 100:.1f}% of the nominal frames are "
                f"usable stereo pairs (minimum {MIN_PAIR_COVERAGE * 100:.0f}%); the camera "
                f"stalled for {assoc.longest_gap_s:.1f}s at worst")
        if span_s < MIN_RUN_S:
            raise ValueError(
                f"pair '{pair}': longest continuous run is {span_s:.1f}s, below the "
                f"{MIN_RUN_S}s a VIO stage can initialise and track through")
        if assoc.coverage < COVERAGE_WARN:
            logger.warning(f"pair '{pair}': coverage {assoc.coverage * 100:.1f}% "
                           f"({assoc.n_left_orphans}L/{assoc.n_right_orphans}R orphan frames)")

        t_left = t_sel - origin
        # True only when the association is a plain prefix of BOTH streams and
        # nothing was clipped -- then the raw pair is still a pure stream copy.
        stream_copyable = (
            bool(np.array_equal(idx_l, np.arange(n)))
            and bool(np.array_equal(idx_r, np.arange(n)))
            # reordering would make `-frames:v n -c copy` cut in the wrong order
            and vl.has_b_frames == 0 and vr.has_b_frames == 0)
        logger.info(
            f"pair '{pair}': {n} aligned pairs, start_skew {start_skew_ms:.3f} ms -> residual "
            f"{assoc.residual_ms:.3f} ms (modal index offset {assoc.modal_offset}), "
            f"coverage {assoc.coverage * 100:.1f}%, "
            f"{'stream-copy' if stream_copyable else 're-encode (frames were cut)'}")

        # Downscale only above 1080p. This device is exactly 1080p so it never
        # triggers today; a future 4K RoboCap would be brought down here.
        scale_to = None
        if vl.height > 1080:
            new_h = 1080
            new_w = int(round(vl.width * new_h / vl.height)) // 2 * 2
            scale_to = (new_w, new_h)
            logger.info(f"pair '{pair}': {vl.width}x{vl.height} > 1080p, scaling to {new_w}x{new_h}")

        # Raw trio. Stream copy whenever the association is a plain prefix of
        # both streams (no generation loss, the common case); otherwise the raw
        # pair must be re-encoded from the aligned frames, because `-c copy` can
        # only cut the TAIL and this device codes a 30-frame GOP. Deferred until
        # after the rectification maps are known so both come out of ONE decode.
        src_bitrate = max([b for b in (vl.bitrate_bps, vr.bitrate_bps) if b], default=None)
        # NOTE: the raw trio, sbs and frame_timestamps.csv are all written AFTER
        # the decode pass below, because a short decode (the container
        # overcounts by one on this recorder) can shorten the package and every
        # output has to agree on the final count.

        report = {
            "artifacts": {
                "raw_left": name("left_raw.mp4").name,
                "raw_right": name("right_raw.mp4").name,
                "rectified_left": None,
                "rectified_right": None,
                "frame_timestamps": name("frame_timestamps.csv").name,
                "calibration": None,
            },
            "left_source": left_src.name, "right_source": right_src.name,
            "frames_left": len(pts_l), "frames_right": len(pts_r), "frames_written": n,
            "fps": vl.fps, "duration_s": vl.duration_s,
            "video_start_device_s": vl.start_device_s,
            "video_start_rebased_s": float(t_left[0]),
            # Split out so the three defects are triageable instead of collapsed
            # into one number: how far apart the eyes STARTED, how far apart the
            # emitted pairs actually are, and where the timeline broke.
            "start_skew_ms": start_skew_ms,
            "residual_skew_ms": assoc.residual_ms,
            "residual_skew_p95_ms": assoc.residual_p95_ms,
            "modal_index_offset": assoc.modal_offset,
            "left_orphan_frames": assoc.n_left_orphans,
            "right_orphan_frames": assoc.n_right_orphans,
            "coverage": assoc.coverage,
            "longest_gap_s": assoc.longest_gap_s,
            "discarded_runs": dropped_runs,
            "frames_outside_imu_coverage": n_uncovered,
            "imu_uncovered_head_frames": head_uncovered,
            "imu_uncovered_tail_frames": tail_uncovered,
            "raw_stream_copied": stream_copyable,
            "span_s": span_s,
            "rectified": False,
        }

        rect_maps = None
        n_emitted = n          # decode may end early; every output follows this
        calib = calib_source = None
        if calib_path is not None:
            if isinstance(calib_path, tuple):
                calib = load_composed_calibration(calib_path[0], calib_path[1], logger)
                calib_source = f"{calib_path[0].name} + {calib_path[1].name}"
                # cos(angle between the two optical axes) is R[2,2].
                axes_deg = float(np.degrees(np.arccos(
                    np.clip(calib.R[2, 2], -1.0, 1.0))))
                if axes_deg > MAX_COMPOSED_AXES_ANGLE_DEG:
                    logger.warning(
                        f"pair '{pair}': composed optical axes diverge by "
                        f"{axes_deg:.1f} deg (limit "
                        f"{MAX_COMPOSED_AXES_ANGLE_DEG:.0f}) -- these cameras "
                        f"do not share a view; emitting raw-only")
                    report["composed_calibration_rejected"] = {
                        "source": calib_source,
                        "optical_axes_angle_deg": axes_deg,
                        "baseline_mm": float(np.linalg.norm(calib.T) * 1000),
                    }
                    calib = None
            else:
                calib = load_akai_calibration(calib_path, logger)
                calib_source = calib_path.name
        if calib is not None:
            # The remap tables are built at the CALIBRATION's resolution, and the
            # encoders are told the VIDEO's. If those disagree, cv2.remap hands
            # the raw pipe frames of the wrong size and ffmpeg silently re-frames
            # the byte stream -- a 1920x1080 map against 160x120 video yields 108
            # garbage "frames" per real frame, in a file that still declares the
            # right dimensions. validate_output's frame-count check would catch
            # it eventually; fail here instead, where the reason is obvious.
            if (calib.width, calib.height) != (vl.width, vl.height):
                raise ValueError(
                    f"pair '{pair}': {calib_source} is calibrated for "
                    f"{calib.width}x{calib.height} but the video is {vl.width}x{vl.height}. "
                    f"Rectifying across that mismatch produces a corrupt stream, not a "
                    f"rescaled one -- intrinsics must be scaled to the source first.")
            refinement = None
            if getattr(config, "refine_extrinsics", False):
                # v1 guarded this with `assoc.residual_ms > 1 ms`, which is DEAD
                # CODE: associate_frames only accepts a pair when the error is
                # already <= that tolerance, so the branch can never be taken.
                # The real fix is to sample the correspondences from the
                # association itself.
                refinement = refine_extrinsics(
                    left_src, right_src, calib, vl.duration_s, logger,
                    idx_left=idx_l, idx_right=idx_r, t_pair=t_sel)
                report["extrinsic_refinement"] = vars(refinement)
                report["extrinsic_refinement"]["correspondence_source"] = "association"
            rect_details: dict = {}
            rect_maps = build_rectification_maps(
                calib, logger,
                target_hfov_deg=getattr(config, "rectified_hfov_degrees", DEFAULT_RECTIFIED_HFOV_DEG),
                rotation_override=(
                    np.asarray(refinement.refined_rotation, dtype=np.float64)
                    if refinement and refinement.applied else None
                ),
                details=rect_details,
            )
            n_emitted = emit_aligned_streams(
                left_src, right_src, idx_l, idx_r,
                vl.width, vl.height, vl.fps, config, logger,
                raw_left=None if stream_copyable else name("left_raw.mp4"),
                raw_right=None if stream_copyable else name("right_raw.mp4"),
                maps=rect_maps,
                rect_left=name("left_rectified.mp4"),
                rect_right=name("right_rectified.mp4"),
                rect_sbs=name("sbs_rectified.mp4") if is_primary else None,
                scale_to=scale_to, source_bitrate_bps=src_bitrate,
            )
            build_calibration_json(calib, rect_maps, logger, refinement).save(
                name("calibration.json")
            )
            report["artifacts"].update({
                "rectified_left": name("left_rectified.mp4").name,
                "rectified_right": name("right_rectified.mp4").name,
                "calibration": name("calibration.json").name,
            })
            hh, vv, dd = source_fov_deg(calib.K1, calib.D1, calib.width, calib.height, strict=False)
            # Diagnostics only: a failure here is recorded, never fatal.
            try:
                rect_diagnostics = {
                    **{k: v for k, v in rect_details.items() if k != "method"},
                    **rectification_report(calib, rect_maps, rect_details.get("method", "?")),
                }
            except Exception as exc:  # noqa: BLE001 -- diagnostics must never fail the pair
                logger.warning(f"pair '{pair}': rectification diagnostics failed: {exc}")
                rect_diagnostics = {**rect_details, "error": f"{type(exc).__name__}: {exc}"}
            report.update({
                "rectified": True,
                "calibration_source": calib_source,
                "raw_baseline_mm": float(np.linalg.norm(calib.T) * 1000),
                "rectified_baseline_mm": rect_maps.baseline_m * 1000,
                "rectified_focal_px": float(rect_maps.P1[0, 0]),
                "source_fov_deg": {"h": hh, "v": vv, "d": dd},
                # Per-eye border/lens diagnostics, known before encoding: what
                # rect_maps implies about the delivered video. `method` names
                # which construction (OpenCV or the Bouguet fallback) built it;
                # the rest of rect_details (cover_zoom, degenerate_reason, ...)
                # rides alongside.
                "rectification_diagnostics": rect_diagnostics,
            })
        else:
            # No calibration -> no rectification pass, so if the raw pair still
            # needs re-aligning it gets its own decode here.
            if not stream_copyable:
                n_emitted = emit_aligned_streams(
                    left_src, right_src, idx_l, idx_r,
                    vl.width, vl.height, vl.fps, config, logger,
                    raw_left=name("left_raw.mp4"), raw_right=name("right_raw.mp4"),
                    maps=None, scale_to=scale_to, source_bitrate_bps=src_bitrate,
                )
            # Documented, deliberate hole -- see the module docstring. Two
            # distinct reasons land here and the marker must not conflate
            # them: no calibration was found at all, versus per-camera files
            # WERE found but their composed geometry is not a stereo pair.
            rejected = report.get("composed_calibration_rejected")
            if rejected:
                why = (
                    f"# '{pair}' pair: NOT RECTIFIED — calibration present but unusable\n\n"
                    f"Per-camera calibrations WERE found and composed through the shared\n"
                    f"IMU ({rejected['source']}), but the composed geometry says the two\n"
                    f"cameras' optical axes diverge by "
                    f"{rejected['optical_axes_angle_deg']:.1f} deg (limit "
                    f"{MAX_COMPOSED_AXES_ANGLE_DEG:.0f} deg) — they do not share a field\n"
                    f"of view, so there is no stereo geometry to rectify. Either the\n"
                    f"cameras genuinely face different directions (mono-only pair), or\n"
                    f"the two mono cam-IMU extrinsics are wrong — ask the vendor which.\n"
                    f"Details: chunk_report.json -> pairs.{pair}.composed_calibration_rejected.\n\n"
                )
            else:
                why = (
                    f"# '{pair}' pair: INCOMPLETE PACKAGE\n\n"
                    f"No calibration shipped for this stereo pair. Accepted forms: a stereo\n"
                    f"`calibration_{pair}.json`, or the per-camera pair\n"
                    f"`calibration_left_{pair}.json` + `calibration_right_{pair}.json`\n"
                    f"(composed through the shared IMU).\n\n"
                )
            (name("MISSING_CALIBRATION.md")).write_text(
                why
                + f"Consequences:\n"
                f"- no `{name('calibration.json').name}` in this directory\n"
                f"- no `{name('left_rectified.mp4').name}` / "
                f"`{name('right_rectified.mp4').name}`\n\n"
                f"Present and trustworthy: `{name('left_raw.mp4').name}`, "
                f"`{name('right_raw.mp4').name}`,\n"
                f"`imu.csv`, `{name('frame_timestamps.csv').name}`, `meta.json`.\n\n"
                + ("Remediation: a corrected (jointly calibrated) stereo calibration for\n"
                   "this pair makes rectification turn on automatically on re-run.\n"
                   if rejected else
                   "To complete this pair, obtain a Kalibr calibration for it from the\n"
                   "device vendor and re-run normalization.\n")
            )
            if rejected:
                logger.warning(
                    f"pair '{pair}': calibration present but composed geometry is not a "
                    f"stereo pair ({rejected['optical_axes_angle_deg']:.1f} deg) -> raw only")
            else:
                logger.warning(f"pair '{pair}': no calibration -> raw only, package is incomplete")

        # ── finalise on the count actually emitted ───────────────────────────
        if n_emitted < n:
            logger.warning(
                f"pair '{pair}': decode yielded {n_emitted} of {n} associated pairs; "
                f"truncating the whole package to {n_emitted}")
            idx_l, idx_r, t_sel = idx_l[:n_emitted], idx_r[:n_emitted], t_sel[:n_emitted]
            t_left = t_sel - origin
            n = n_emitted
            span_s = float(t_sel[-1] - t_sel[0]) if n > 1 else 0.0
            if n < 2 or span_s < MIN_RUN_S:
                raise ValueError(
                    f"pair '{pair}': only {n} frame(s) spanning {span_s:.1f}s survived decoding")
        if stream_copyable:
            # cut to the final n, which a short decode may have reduced
            remux(left_src, name("left_raw.mp4"), n)
            remux(right_src, name("right_raw.mp4"), n)
        write_frame_timestamps_csv([float(v) for v in t_left[:n]], name("frame_timestamps.csv"))
        report["frames_written"] = n
        report["span_s"] = span_s
        # sbs_raw is built from the ALIGNED raw pair, so it comes after whichever
        # branch produced left_raw/right_raw. Core pair only: for aux pairs it
        # would be 57% of the bytes with no reader.
        if is_primary:
            encode_sbs_raw(name("left_raw.mp4"), name("right_raw.mp4"),
                           name("sbs_raw.mp4"), config, src_bitrate, n, vl.fps)

        ow, oh = scale_to if scale_to else (vl.width, vl.height)
        if not is_primary:
            # Aux pairs carry no meta of their own — the root meta.json's
            # aux_streams block plus chunk_report.json record their facts.
            return None, report
        meta = MetaJson(
            schema_version=SCHEMA_VERSION,
            device=DeviceInfo(
                device_type_from_folder_path="bitrobot",
                device_id=imu.metadata.get("deviceid"),
                model_from_device_metadata=(
                    f"{imu.metadata.get('product', 'robocap')} "
                    f"(camera {imu.metadata.get('camera', '?')}, imu {imu.metadata.get('imu', '?')})"
                ),
                firmware_version=imu.metadata.get("version"),
                stereo_pair=pair,
            ),
            recording=RecordingInfo(
                width=ow, height=oh, fps=vl.fps,
                duration_seconds=vl.duration_s, frame_count=n,
                is_stereo=True, codec=vl.codec,
            ),
            source=SourceInfo(
                original_format="mp4",
                original_files=sorted([left_src.name, right_src.name,
                                       chunk.imu_calibrated.name]
                                      + ([p.name for p in (
                                          calib_path if isinstance(calib_path, tuple)
                                          else (calib_path,))] if calib_path else [])),
                total_bytes=sum(p.stat().st_size for p in (left_src, right_src, chunk.imu_calibrated)),
                # The recorder clock is a free-running monotonic counter, not a
                # wall clock, and no file carries a real UTC capture time.
                # Fabricating one is exactly what the v2 schema forbids.
                recorded_utc=None,
                normalized_utc=datetime.now(timezone.utc).isoformat(),
                bitrate_mbps=round(src_bitrate / 1_000_000.0, 2) if src_bitrate else None,
            ),
            encoding=EncodingInfo(
                codec=config.video_codec, crf=config.video_crf,
                preset=config.video_preset, lossless=False,
            ),
            processing_metrics=_processing_metrics(timer),
        )
        # NOT saved here: normalize() attaches the aux_streams inventory
        # (which is only known once every pair has run) and saves it.
        return meta, report
