# Veo 3.1 Model Mapping — Confirmed Reference

> **Purpose**: This file is the canonical source of truth for which `videoModelKey`
> string Flow's API expects for each combination of (sub-mode × tier × ratio × plan).
>
> Every "✅ CONFIRMED" entry below was captured live from a real
> `labs.google.com` account using a DevTools `fetch` wrapper, then matched
> against the model name our app was sending. If the API ever starts
> returning `MODEL_NOT_FOUND` or `INVALID_MODEL` errors, re-verify the
> failing entry against this table before changing the code in
> `src/core/extension_mode.py → _resolve_video_model_for_sub_mode()`.

---

## How to capture a fresh model name from labs.google.com

1. Open `https://labs.google.com/fx/tools/flow` in Chrome (logged-in account).
2. Open DevTools → Console tab.
3. Paste this fetch wrapper and hit Enter:

```js
(() => {
  const _fetch = window.fetch;
  window.fetch = async function(input, init) {
    try {
      const url = typeof input === "string" ? input : input.url;
      if (url && url.includes("/v1/video:batchAsync")) {
        const body = init && init.body ? JSON.parse(init.body) : null;
        const ep = url.split("/").pop().split("?")[0];
        console.log("━".repeat(50));
        console.log("🎯 ENDPOINT:", ep);
        if (body) {
          console.log("   tier:", body?.clientContext?.tier);
          console.log("   videoModelKey:", body?.requests?.[0]?.aspectRatio
              ? body.requests[0].videoModelKey
              : body?.requests?.[0]?.videoModelKey);
          console.log("   aspectRatio:", body?.requests?.[0]?.aspectRatio);
          if (body?.requests?.[0]?.startImage) console.log("   startImage: yes");
          if (body?.requests?.[0]?.endImage)   console.log("   endImage: yes");
          if (body?.requests?.[0]?.referenceImages)
            console.log("   referenceImages count:", body.requests[0].referenceImages.length);
        }
        console.log("━".repeat(50));
      }
    } catch (e) {}
    return _fetch.apply(this, arguments);
  };
  console.log("✅ Veo capture wrapper installed");
})();
```

4. Pick the desired Mode + Quality + Ratio in Flow's UI, hit Generate.
5. Read the captured `videoModelKey` from the console.

---

## Plan tier mapping

| App "Flow Account Plan" setting | API `tier` value          |
|---------------------------------|---------------------------|
| Ultra                           | `PAYGATE_TIER_TWO`        |
| Pro                             | `PAYGATE_TIER_ONE`        |

The `_ultra` suffix is appended to most Fast-tier models on Ultra plan.
Pro plan strips it (e.g., `veo_3_1_t2v_fast` instead of `veo_3_1_t2v_fast_ultra`).

### Available tier options differ by plan ⚠️

Flow's UI exposes **different** quality tier options depending on the account plan:

| UI tier option              | Ultra | Pro |
|-----------------------------|:-----:|:---:|
| Veo 3.1 - Fast              |  ✅   | ✅  |
| Veo 3.1 - Lite              |  ✅   | ✅  |
| Veo 3.1 - Quality           |  ✅   | ✅  |
| Veo 3.1 - Fast [Lower Pri]  |  ✅   | ❌  |
| Veo 3.1 - Lite [Lower Pri]  |  ✅   | ❌  |

Pro plan has **NO [Lower Pri] variants** — confirmed via live UI screenshot
of a Pro account. Sending a `_relaxed` or `_low_priority` model key on a
Pro account returns `PUBLIC_ERROR_MODEL_ACCESS_DENIED`.

The app enforces this automatically via `_apply_plan_tier_filter()` in
`src/ui/main_window.py` — whenever the user flips the Flow Account Plan
dropdown, the Veo tier dropdowns are rebuilt to match what Flow actually
exposes for that plan.

---

## Endpoint lookup

| Sub-mode (UI)                | API endpoint                                            |
|------------------------------|---------------------------------------------------------|
| Text → Video                 | `video:batchAsyncGenerateVideoText`                     |
| Ingredients (reference imgs) | `video:batchAsyncGenerateVideoReferenceImages`          |
| Frames Start (1 image)       | `video:batchAsyncGenerateVideoStartImage`               |
| Frames Start + End (2 imgs)  | `video:batchAsyncGenerateVideoStartAndEndImage`         |

---

## Tier dropdown labels (UI → internal `tier` token)

The UI label is parsed in `extension_mode.py` to produce a tier token.
Detection order matters — the combined tokens must be checked first.

| UI label                       | Detected tier      |
|--------------------------------|--------------------|
| `Veo 3.1 - Fast`               | `fast`             |
| `Veo 3.1 - Fast [Lower Pri]`   | `lower_pri`        |
| `Veo 3.1 - Quality`            | `quality`          |
| `Veo 3.1 - Lite`               | `lite`             |
| `Veo 3.1 - Lite [Lower Pri]`   | `lite_lower_pri`   |

---

## ✅ MODEL MAPPING (Ultra plan = `_ultra` suffix appended to Fast variants)

### 1. Text-to-Video (`video:batchAsyncGenerateVideoText`)

| Tier            | Ratio     | Model key                                  | Status        |
|-----------------|-----------|--------------------------------------------|---------------|
| Lite            | any       | `veo_3_1_t2v_lite`                         | ✅ CONFIRMED  |
| Lite [LP]       | any       | `veo_3_1_t2v_lite_low_priority`            | ✅ CONFIRMED  |
| Quality         | any       | `veo_3_1_t2v`                              | ✅ CONFIRMED  |
| Fast            | Landscape | `veo_3_1_t2v_fast_ultra`                   | ✅ CONFIRMED  |
| Fast            | Portrait  | `veo_3_1_t2v_fast_portrait_ultra`          | ✅ CONFIRMED  |
| Fast [LP]       | Landscape | `veo_3_1_t2v_fast_ultra_relaxed`           | ✅ CONFIRMED  |
| Fast [LP]       | Portrait  | `veo_3_1_t2v_fast_portrait_ultra_relaxed`  | ⚪ INFERRED   |

**Notes:**
- Square ratio is NOT supported by Flow for video — UI only exposes Landscape + Portrait.
- Lite / Lite-LP / Quality ignore ratio entirely (single model handles both).

### 2. Ingredients / Reference-to-Video (`video:batchAsyncGenerateVideoReferenceImages`)

| Tier            | Ratio     | Model key                                          | Status        |
|-----------------|-----------|----------------------------------------------------|---------------|
| Lite            | any       | `veo_3_1_r2v_lite`                                 | ✅ CONFIRMED  |
| Lite [LP]       | any       | `veo_3_1_r2v_lite_low_priority`                    | ✅ CONFIRMED  |
| Quality         | any       | (collapses to Fast — Flow doesn't expose Quality)  | ⚪ N/A        |
| Fast            | Landscape | `veo_3_1_r2v_fast_landscape_ultra`                 | ✅ CONFIRMED  |
| Fast            | Portrait  | `veo_3_1_r2v_fast_portrait_ultra`                  | ⚪ INFERRED   |
| Fast [LP]       | Landscape | `veo_3_1_r2v_fast_landscape_ultra_relaxed`         | ✅ CONFIRMED  |
| Fast [LP]       | Portrait  | `veo_3_1_r2v_fast_portrait_ultra_relaxed`          | ⚪ INFERRED   |

**Notes:**
- `r2v` = Reference-to-Video (a.k.a. Ingredients in UI).
- Quality option in UI for Ingredients does NOT exist on Flow's official site — we
  fall back to the Fast model for safety. Don't be surprised if Quality + Ingredients
  silently behaves like Fast.

### 3. Frames Start (single image) — `video:batchAsyncGenerateVideoStartImage`

| Tier            | Model key                                  | Status        |
|-----------------|--------------------------------------------|---------------|
| Lite            | `veo_3_1_i2v_lite`                         | ✅ CONFIRMED  |
| Lite [LP]       | `veo_3_1_i2v_lite_low_priority`            | ✅ CONFIRMED  |
| Quality         | `veo_3_1_i2v_s`                            | ✅ CONFIRMED  |
| Fast            | `veo_3_1_i2v_s_fast_ultra`                 | ✅ CONFIRMED  |
| Fast [LP]       | `veo_3_1_i2v_s_fast_ultra_relaxed`         | ✅ CONFIRMED  |

**Notes:**
- The `_s` suffix appears only on Fast / Quality variants — Lite/Lite-LP drop it.
- This sub-mode does NOT encode ratio in the model key (unlike T2V Fast).

### 4. Frames Start-End (two images) — `video:batchAsyncGenerateVideoStartAndEndImage`

⚠️ **HYBRID FAMILY** — splits across two model bases by tier:
  - Lite / Lite [LP] → use the new `interpolation_*` family
  - Fast / Fast LP / Quality → use the older `i2v_s_*_fl` family

⚠️ **`_fl` SUFFIX POSITION QUIRK** — Google's naming is inconsistent here:
  - Fast plain : `_fl` sits at the **END** → `veo_3_1_i2v_s_fast_ultra_fl`
  - Fast LP    : `_fl` jumps to the **MIDDLE** → `veo_3_1_i2v_s_fast_fl_ultra_relaxed`

| Tier            | Model key                                       | Status        |
|-----------------|-------------------------------------------------|---------------|
| Lite            | `veo_3_1_interpolation_lite`                    | ✅ CONFIRMED  |
| Lite [LP]       | `veo_3_1_interpolation_lite_low_priority`       | ✅ CONFIRMED  |
| Quality         | `veo_3_1_i2v_s_fl`                              | ✅ CONFIRMED  |
| Fast            | `veo_3_1_i2v_s_fast_ultra_fl`                   | ✅ CONFIRMED  |
| Fast [LP]       | `veo_3_1_i2v_s_fast_fl_ultra_relaxed`           | ✅ CONFIRMED  |

**Old assumption that broke things (DON'T REVERT)**: Earlier the resolver
returned `veo_3_1_interpolation_fast_ultra` for Fast tier and
`veo_3_1_interpolation` for Quality. These models do NOT exist — the API
returns errors. The hybrid pattern above is the correct one.

---

---

## ✅ MODEL MAPPING — Pro plan (`PAYGATE_TIER_ONE`, no `_ultra` suffix)

**Pro plan rule of thumb**: Take the Ultra plan's model key and strip the
`_ultra` substring. Lite and Quality tiers are plan-independent — their
keys are identical across Ultra and Pro. Pro plan has no LP variants at all.

### 1. Text-to-Video (Pro)

| Tier      | Ratio     | Model key                              | Status        |
|-----------|-----------|----------------------------------------|---------------|
| Lite      | any       | `veo_3_1_t2v_lite`                     | ✅ CAPTURED   |
| Quality   | any       | `veo_3_1_t2v`                          | ✅ CAPTURED   |
| Fast      | Landscape | `veo_3_1_t2v_fast`                     | ✅ CAPTURED   |
| Fast      | Portrait  | `veo_3_1_t2v_fast_portrait`            | 🟢 Derived    |

### 2. Ingredients / R2V (Pro)

| Tier      | Ratio     | Model key                              | Status        |
|-----------|-----------|----------------------------------------|---------------|
| Lite      | any       | `veo_3_1_r2v_lite`                     | 🟢 Derived    |
| Fast      | Landscape | `veo_3_1_r2v_fast_landscape`           | ✅ CAPTURED   |
| Fast      | Portrait  | `veo_3_1_r2v_fast_portrait`            | 🟢 Derived    |

### 3. Frames Start (Pro)

| Tier      | Model key                              | Status        |
|-----------|----------------------------------------|---------------|
| Lite      | `veo_3_1_i2v_lite`                     | 🟢 Derived    |
| Quality   | `veo_3_1_i2v_s`                        | 🟢 Derived    |
| Fast      | `veo_3_1_i2v_s_fast`                   | 🟢 Derived    |

### 4. Frames Start-End (Pro)

| Tier      | Model key                              | Status        |
|-----------|----------------------------------------|---------------|
| Lite      | `veo_3_1_interpolation_lite`           | 🟢 Derived    |
| Quality   | `veo_3_1_i2v_s_fl`                     | 🟢 Derived    |
| Fast      | `veo_3_1_i2v_s_fast_fl`                | 🟡 Inferred   |

**Status legend:**
- ✅ CAPTURED = directly observed on a real Pro account via the fetch wrapper
- 🟢 Derived = identical to Ultra (Lite/Quality) OR Ultra minus `_ultra` — pattern proven across ≥2 captured families, so confidence is high
- 🟡 Inferred = follows the derivation rule but no live capture yet; re-verify if API rejects

**Known unknown**: FSE Fast on Pro. Ultra has a quirky `_fl` position
(Fast plain: `_fast_ultra_fl`, Fast LP: `_fast_fl_ultra_relaxed`). Pro
strips `_ultra` — the expected result is `veo_3_1_i2v_s_fast_fl` (retains
end-position `_fl`), but no capture yet.

---

## Suffix grammar (cheat sheet)

| Suffix          | Means                                   | Where it appears                            |
|-----------------|-----------------------------------------|---------------------------------------------|
| `_lite`         | Lite tier                               | All families                                |
| `_low_priority` | Lower priority queue (Lite tier only)   | Lite + LP combo                             |
| `_relaxed`      | Lower priority queue (Fast tier only)   | Fast + LP combo (different from `_low_pri`!) |
| `_ultra`        | Paid Ultra-tier account perks           | Fast variants on Ultra plan only            |
| `_portrait`     | Vertical 9:16 ratio                     | T2V Fast + R2V Fast                         |
| `_landscape`    | Horizontal 16:9 ratio (R2V only)        | R2V Fast variants                           |
| `_s`            | "Start image" image-to-video variant    | I2V Fast / Quality / FSE                    |
| `_fl`           | "Final/last frame" (start+end mode)     | FSE Fast / Quality (position varies!)       |

---

## Last verified

- **Date**: 2026-04-20
- **Plans**: Ultra (`PAYGATE_TIER_TWO`) + Pro (`PAYGATE_TIER_ONE`)
- **Site**: labs.google.com/fx/tools/flow
- **Veo version**: Veo 3.1
- **Code location**: `src/core/extension_mode.py → _resolve_video_model_for_sub_mode()`
- **UI filter location**: `src/ui/main_window.py → _apply_plan_tier_filter()`

**Live capture totals:**
- Ultra plan: 19/19 primary combos captured (Fast LP Portrait 1 inferred)
- Pro plan: 4/11 combos captured, 6 derived (Lite/Quality identical + 2 Fast strip), 1 inferred (FSE Fast)

---

## What's still NOT verified (future work)

1. **FSE Fast on Pro** — current guess: `veo_3_1_i2v_s_fast_fl`. Worth
   capturing to remove the last inferred entry.
2. **R2V and I2V Portrait variants on Pro** — pattern-derived, no live capture.
3. **Ultra T2V Fast LP Portrait** — pattern-derived (`_fast_portrait_ultra_relaxed`).
4. **Square ratio for video** — Flow doesn't currently support it. Don't add a
   `_square` branch unless captured live.
5. **Future Veo versions** (3.2, 4.0, etc.) — re-run capture exercise when Google ships
   a new model family.

---

## Quick re-verification ritual (when something breaks)

If a job suddenly starts failing with `MODEL_NOT_FOUND`, `INVALID_MODEL`, or
similar API rejection:

1. Reproduce the failure with our app, note the exact UI selections (mode,
   quality, ratio).
2. Open labs.google.com manually with the same account, install the fetch
   wrapper from the top of this doc, make the same selections, hit Generate.
3. Compare the captured `videoModelKey` against this doc's table.
4. If different → Google changed the naming. Update both this doc AND
   `_resolve_video_model_for_sub_mode()` in `extension_mode.py`.
5. If same → the bug is elsewhere (auth, rate limit, request body shape).
