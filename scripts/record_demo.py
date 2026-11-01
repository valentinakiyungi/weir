#!/usr/bin/env python3
"""Record the site's gateway simulation as an animated GIF for the README.

Plays three scripted scenes (a fresh page load each), with a caption strip
above the simulator, records them with Playwright and stitches them into
docs/assets/weir-simulation.gif with ffmpeg.

  pip install playwright && playwright install chromium   # once
  python scripts/record_demo.py
"""

from __future__ import annotations

import asyncio
import shutil
import subprocess
import tempfile
import time
from pathlib import Path

from playwright.async_api import async_playwright

ROOT = Path(__file__).resolve().parent.parent
SITE = (ROOT / "site" / "index.html").as_uri()
OUT = ROOT / "docs" / "assets" / "weir-simulation.gif"
VIEW = {"width": 1280, "height": 900}
FPS = 10
WIDTH = 800

CAPTION_JS = """
(text) => {
  let el = document.getElementById('rec-cap');
  if (!el) {
    el = document.createElement('div');
    el.id = 'rec-cap';
    el.style.cssText = 'font:600 22px/1.3 Archivo,sans-serif;color:var(--ink);' +
      'padding:14px 4px;min-height:62px;display:block;text-wrap:balance';
    const sim = document.querySelector('.sim');
    sim.style.marginTop = '0';
    sim.parentNode.insertBefore(el, sim);
  }
  el.innerHTML = text;
}
"""

# Each scene: list of (seconds from scene start, action, argument)
SCENES = [
    [
        (0.0, "caption", "<b>Reserve first (Weir).</b> Three teams share one provider key, "
                         "each with its own limits and daily budget."),
        (3.0, "caption", "<b>A burst of 72 requests.</b> Weir lowers <code>max_tokens</code> "
                         "near the limit (amber) and refuses the rest. Nothing passes the line."),
        (3.2, "click", "#burst"),
        (11.0, "end", None),
    ],
    [
        (0.0, "click", "#mode-naive"),
        (0.0, "caption", "<b>Check, then charge.</b> The same burst against a naive limiter..."),
        (1.5, "click", "#burst"),
        (4.0, "caption", "<b>Check, then charge.</b> Every request passed the check before any "
                         "was charged, so budgets overshoot (red)."),
        (10.0, "end", None),
    ],
    [
        (0.0, "caption", "<b>Redis goes offline.</b> Requests keep flowing under per-process "
                         "rate limits instead of failing."),
        (1.0, "click", "#redis"),
        (6.5, "caption", "<b>Redis is back.</b> Shared limits and budgets resume automatically."),
        (6.7, "click", "#redis"),
        (10.0, "end", None),
    ],
]


async def record_scene(browser, steps, workdir: Path, idx: int) -> tuple[Path, dict, float]:
    ctx = await browser.new_context(viewport=VIEW, color_scheme="light",
                                    record_video_dir=str(workdir / f"s{idx}"),
                                    record_video_size=VIEW)
    t_open = time.monotonic()
    page = await ctx.new_page()
    await page.goto(SITE)
    await page.wait_for_timeout(600)
    await page.evaluate(CAPTION_JS, "")
    await page.add_style_tag(content='.actions{visibility:hidden}')
    box = await page.evaluate("""() => {
        const cap = document.getElementById('rec-cap'), sim = document.querySelector('.sim');
        window.scrollTo(0, cap.getBoundingClientRect().top + scrollY - 12);
        const a = cap.getBoundingClientRect(), b = sim.getBoundingClientRect();
        return {x: Math.floor(b.left) - 12, y: Math.floor(a.top),
                w: Math.ceil(b.width) + 24, h: Math.ceil(b.bottom - a.top) + 14};
    }""")
    await page.wait_for_timeout(300)
    start = time.monotonic()
    for at, action, arg in steps:
        delay = at - (time.monotonic() - start)
        if delay > 0:
            await page.wait_for_timeout(delay * 1000)
        if action == "caption":
            await page.evaluate(CAPTION_JS, arg)
        elif action == "click":
            await page.click(arg)
    offset = start - t_open
    video = await page.video.path()
    await ctx.close()
    return Path(video), box, offset


async def record_all(work: Path) -> list[tuple[Path, dict, float]]:
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        clips = [await record_scene(browser, s, work, i) for i, s in enumerate(SCENES)]
        await browser.close()
    return clips


def main() -> None:
    if not shutil.which("ffmpeg"):
        raise SystemExit("ffmpeg is required")
    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)
        clips = asyncio.run(record_all(work))

        parts = []
        for i, ((video, box, offset), steps) in enumerate(zip(clips, SCENES, strict=True)):
            dur = steps[-1][0]
            part = work / f"part{i}.mp4"
            crop = f"crop={box['w']}:{box['h']}:{max(0, box['x'])}:{max(0, box['y'])}"
            subprocess.run(["ffmpeg", "-loglevel", "error", "-y", "-ss", f"{offset:.2f}",
                            "-i", str(video), "-t", str(dur), "-vf",
                            f"{crop},fps={FPS},scale={WIDTH}:-2:flags=lanczos",
                            "-pix_fmt", "yuv420p", str(part)], check=True)
            parts.append(part)

        listing = work / "parts.txt"
        listing.write_text("".join(f"file '{p}'\n" for p in parts))
        joined = work / "joined.mp4"
        subprocess.run(["ffmpeg", "-loglevel", "error", "-y", "-f", "concat", "-safe", "0",
                        "-i", str(listing), "-c", "copy", str(joined)], check=True)
        OUT.parent.mkdir(parents=True, exist_ok=True)
        palette = work / "palette.png"
        subprocess.run(["ffmpeg", "-loglevel", "error", "-y", "-i", str(joined), "-vf",
                        "palettegen=max_colors=64:stats_mode=diff", str(palette)], check=True)
        subprocess.run(["ffmpeg", "-loglevel", "error", "-y", "-i", str(joined), "-i",
                        str(palette), "-lavfi",
                        "paletteuse=dither=bayer:bayer_scale=5:diff_mode=rectangle",
                        "-loop", "0", str(OUT)], check=True)
        shutil.copy(joined, OUT.with_suffix(".mp4"))
    print(f"wrote {OUT} ({OUT.stat().st_size / 1e6:.1f} MB)")


if __name__ == "__main__":
    main()
