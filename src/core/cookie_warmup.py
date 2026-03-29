import asyncio
import inspect
import random


SEARCH_QUERIES = [
    "best ai image generator free 2026",
    "how to write good ai image prompts",
    "ai art generator tips and tricks",
    "google labs ai image tools",
    "nano banana ai model review",
    "ai image generation prompt guide",
    "best prompts for ai art",
    "how to use google flow for images",
    "ai image quality improve tips",
    "free ai tools for graphic design",
    "ai generated art examples 2026",
    "text to image ai comparison",
    "best ai for landscape images",
    "ai portrait generator free",
    "how to generate realistic ai images",
    "graphic design trends 2026",
    "digital art techniques beginners",
    "color theory for digital art",
    "best design software free",
    "how to make professional graphics",
    "typography tips for designers",
    "illustration techniques digital",
    "photo editing tips professional",
    "latest ai news today",
    "best productivity tools 2026",
    "python automation tutorial",
    "machine learning for beginners",
    "cloud computing explained",
    "web development trends 2026",
    "best laptops for design 2026",
    "how to learn programming fast",
    "tech startup ideas 2026",
    "remote work tools best",
    "cybersecurity basics guide",
    "data visualization tools free",
    "open source ai projects",
    "creative coding examples",
    "ux design principles",
]

EXTRA_SITES_POOL = [
    {"url": "https://www.youtube.com", "wait": (3, 5), "name": "YouTube"},
    {"url": "https://news.google.com", "wait": (2, 4), "name": "Google News"},
    {"url": "https://github.com", "wait": (2, 4), "name": "GitHub"},
    {"url": "https://stackoverflow.com", "wait": (2, 4), "name": "StackOverflow"},
    {"url": "https://www.reddit.com", "wait": (2, 4), "name": "Reddit"},
    {"url": "https://www.wikipedia.org", "wait": (2, 4), "name": "Wikipedia"},
    {"url": "https://medium.com", "wait": (2, 4), "name": "Medium"},
    {"url": "https://www.linkedin.com", "wait": (2, 4), "name": "LinkedIn"},
    {"url": "https://www.amazon.com", "wait": (2, 3), "name": "Amazon"},
    {"url": "https://www.nytimes.com", "wait": (2, 3), "name": "NY Times"},
    {"url": "https://maps.google.com", "wait": (2, 3), "name": "Google Maps"},
    {"url": "https://drive.google.com", "wait": (2, 3), "name": "Google Drive"},
    {"url": "https://translate.google.com", "wait": (2, 3), "name": "Google Translate"},
    {"url": "https://www.bbc.com", "wait": (2, 3), "name": "BBC"},
    {"url": "https://www.forbes.com", "wait": (2, 3), "name": "Forbes"},
    {"url": "https://techcrunch.com", "wait": (2, 3), "name": "TechCrunch"},
    {"url": "https://www.theverge.com", "wait": (2, 3), "name": "The Verge"},
    {"url": "https://www.quora.com", "wait": (2, 3), "name": "Quora"},
    {"url": "https://www.pinterest.com", "wait": (2, 3), "name": "Pinterest"},
    {"url": "https://www.imdb.com", "wait": (2, 3), "name": "IMDB"},
]


async def _maybe_await(result):
    if inspect.isawaitable(result):
        return await result
    return result


async def _human_pause(min_seconds, max_seconds):
    await asyncio.sleep(random.uniform(min_seconds, max_seconds))


async def _type_human(page, text):
    for char in str(text or ""):
        await _maybe_await(page.keyboard.type(char))
        await asyncio.sleep(random.uniform(0.03, 0.12))


async def _natural_scroll(page, min_count=2, max_count=4, min_delta=150, max_delta=500):
    for _ in range(random.randint(min_count, max_count)):
        await _maybe_await(page.evaluate(f"window.scrollBy(0, {random.randint(min_delta, max_delta)})"))
        await _human_pause(0.5, 1.5)


async def _move_mouse_randomly(page):
    mouse = getattr(page, "mouse", None)
    if mouse is None:
        return
    try:
        for _ in range(random.randint(1, 3)):
            await _maybe_await(mouse.move(random.randint(100, 900), random.randint(100, 600)))
            await _human_pause(0.3, 0.8)
    except Exception:
        pass


async def _safe_go_back(page):
    try:
        await _maybe_await(page.go_back(timeout=5000))
        await _human_pause(1.0, 2.0)
        return True
    except Exception:
        try:
            await _maybe_await(page.goto("https://www.google.com", timeout=8000))
            await asyncio.sleep(1)
            return False
        except Exception:
            return False


async def _google_search_and_browse(page, log_fn):
    query = random.choice(SEARCH_QUERIES)
    logger = log_fn if callable(log_fn) else (lambda _msg: None)
    logger(f"[WARM-UP] Google Search: '{query}'")
    try:
        await _maybe_await(
            page.goto(
                "https://www.google.com",
                wait_until="domcontentloaded",
                timeout=10000,
            )
        )
        await _human_pause(1.0, 2.0)

        search_box = page.locator('textarea[name="q"]')
        if await _maybe_await(search_box.count()) == 0:
            search_box = page.locator('input[name="q"]')
        if await _maybe_await(search_box.count()) == 0:
            logger("[WARM-UP] Search box not found, skipping search")
            return False

        await _maybe_await(search_box.first.click())
        await _human_pause(0.3, 0.8)
        await _type_human(page, query)
        await _human_pause(0.5, 1.5)
        await _maybe_await(page.keyboard.press("Enter"))
        await _human_pause(2.0, 4.0)

        await _natural_scroll(page, min_count=2, max_count=4, min_delta=200, max_delta=500)

        results = page.locator("h3")
        result_count = int(await _maybe_await(results.count()) or 0)
        if result_count < 2:
            logger("[WARM-UP] No search results found, skipping clicks")
            return True

        max_index = min(result_count - 1, 4)
        clicks_to_do = random.randint(1, 3)
        indices = random.sample(range(0, max_index + 1), min(clicks_to_do, max_index + 1))
        for idx in indices:
            try:
                logger(f"[WARM-UP] Clicking result #{idx + 1}...")
                result_element = results.nth(idx)
                await _maybe_await(result_element.scroll_into_view_if_needed(timeout=3000))
                await _human_pause(0.5, 1.0)
                await _maybe_await(result_element.click())
                await _human_pause(2.0, 5.0)
                await _natural_scroll(page, min_count=2, max_count=5, min_delta=150, max_delta=500)
                await _move_mouse_randomly(page)
                if random.random() > 0.4:
                    await _maybe_await(page.evaluate(f"window.scrollBy(0, -{random.randint(100, 300)})"))
                    await _human_pause(0.5, 1.0)
                await _human_pause(3.0, 8.0)
                await _safe_go_back(page)
                await _maybe_await(page.evaluate(f"window.scrollBy(0, {random.randint(100, 300)})"))
                await _human_pause(0.5, 1.0)
            except Exception as exc:
                logger(f"[WARM-UP] Click #{idx + 1} failed: {str(exc)[:40]}")
                try:
                    await _safe_go_back(page)
                except Exception:
                    pass
                continue

        return True
    except Exception as exc:
        logger(f"[WARM-UP] Google Search failed: {str(exc)[:50]}")
        return False


async def _visit_random_site(page, log_fn, site=None):
    target = site or random.choice(EXTRA_SITES_POOL)
    logger = log_fn if callable(log_fn) else (lambda _msg: None)
    try:
        logger(f"[WARM-UP] Visiting {target['name']}...")
        await _maybe_await(
            page.goto(
                target["url"],
                wait_until="domcontentloaded",
                timeout=10000,
            )
        )
        await _natural_scroll(page, min_count=2, max_count=4, min_delta=150, max_delta=400)
        await _move_mouse_randomly(page)
        await _human_pause(*target["wait"])
        return True
    except Exception:
        return False


async def heavy_cookie_warmup(page, account_name, log_fn, browser_mode="headless", progress_fn=None):
    del browser_mode

    if page is None:
        return False

    logger = log_fn if callable(log_fn) else (lambda _msg: None)
    report_progress = progress_fn if callable(progress_fn) else None
    logger(f"[{account_name}] Full cookie warm-up starting...")

    successful = 0
    if report_progress:
        report_progress(account_name, 0, "Starting warm-up...")

    logger("[WARM-UP] Phase 1: Google Search + Browse...")
    if report_progress:
        report_progress(account_name, 5, "Google Search 1...")
    if await _google_search_and_browse(page, logger):
        successful += 1
    await _human_pause(2.0, 4.0)

    logger("[WARM-UP] Phase 2: Another Google Search + Browse...")
    if report_progress:
        report_progress(account_name, 30, "Google Search 2...")
    if await _google_search_and_browse(page, logger):
        successful += 1
    await _human_pause(2.0, 4.0)

    extra_visits = random.randint(3, 4)
    sites = random.sample(EXTRA_SITES_POOL, extra_visits)
    total_steps = 2 + extra_visits
    for index, site in enumerate(sites):
        logger(f"[WARM-UP] Phase 3: Visiting {site['name']}...")
        if report_progress:
            percent = min(95, 55 + int((index / max(1, extra_visits)) * 40))
            report_progress(account_name, percent, f"Visiting {site['name']}...")
        if await _visit_random_site(page, logger, site=site):
            successful += 1
        await _human_pause(1.0, 3.0)

    if report_progress:
        report_progress(account_name, 100, f"Done! {successful}/{total_steps} activities")
    logger(f"[{account_name}] Full warm-up complete! {successful}/{total_steps} activities done.")
    return successful > 0


async def light_cookie_warmup(page, account_name, log_fn, progress_fn=None):
    if page is None:
        return False

    logger = log_fn if callable(log_fn) else (lambda _msg: None)
    report_progress = progress_fn if callable(progress_fn) else None
    logger(f"[{account_name}] Quick cookie refresh...")

    success = False
    if report_progress:
        report_progress(account_name, 0, "Quick search...")
        report_progress(account_name, 20, "Google Search...")
    if await _google_search_and_browse(page, logger):
        success = True
    if report_progress:
        report_progress(account_name, 60, "Search done")

    if random.random() > 0.5:
        if report_progress:
            report_progress(account_name, 70, "Visiting extra site...")
        await _human_pause(1.0, 3.0)
        if await _visit_random_site(page, logger):
            success = True

    if report_progress:
        report_progress(account_name, 100, "Ready!")
    logger(f"[{account_name}] Cookie refresh done!")
    return success


async def cookie_warmup(page, account_name, log_fn, browser_mode="headless"):
    return await heavy_cookie_warmup(
        page,
        account_name,
        log_fn,
        browser_mode=browser_mode,
        progress_fn=None,
    )
