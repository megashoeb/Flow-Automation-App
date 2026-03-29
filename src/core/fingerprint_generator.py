import hashlib
import json
import random
import re
import time

try:
    from fake_useragent import UserAgent
except Exception:
    UserAgent = None


class FingerprintGenerator:
    """Generate random but internally consistent browser fingerprints per session."""

    CHROME_VERSIONS = [f"{major}.0.0.0" for major in range(131, 146)]

    DEVICE_PROFILES = [
        {
            "platform": "Windows NT 10.0; Win64; x64",
            "navigator_platform": "Win32",
            "ua_platform_name": "Windows",
            "platform_version": "10.0.0",
            "languages": [
                ["en-US", "en"],
                ["en-GB", "en"],
                ["en-US", "en", "hi"],
                ["en-US", "en", "es"],
            ],
            "timezones": [
                "America/New_York",
                "America/Chicago",
                "America/Los_Angeles",
                "America/Denver",
                "Asia/Kolkata",
            ],
        },
        {
            "platform": "Macintosh; Intel Mac OS X 10_15_7",
            "navigator_platform": "MacIntel",
            "ua_platform_name": "macOS",
            "platform_version": "10.15.7",
            "languages": [
                ["en-US", "en"],
                ["en-GB", "en"],
            ],
            "timezones": [
                "America/Los_Angeles",
                "America/Denver",
                "Europe/London",
            ],
        },
        {
            "platform": "Macintosh; Intel Mac OS X 14_0",
            "navigator_platform": "MacIntel",
            "ua_platform_name": "macOS",
            "platform_version": "14.0.0",
            "languages": [
                ["en-US", "en"],
                ["en-GB", "en"],
            ],
            "timezones": [
                "America/Los_Angeles",
                "America/Denver",
                "Europe/London",
            ],
        },
        {
            "platform": "X11; Linux x86_64",
            "navigator_platform": "Linux x86_64",
            "ua_platform_name": "Linux",
            "platform_version": "6.0.0",
            "languages": [
                ["en-US", "en"],
                ["en-GB", "en"],
            ],
            "timezones": [
                "America/Chicago",
                "Europe/London",
            ],
        },
    ]

    SCREENS = [
        {"width": 1920, "height": 1080},
        {"width": 2560, "height": 1440},
        {"width": 1366, "height": 768},
        {"width": 1536, "height": 864},
        {"width": 1440, "height": 900},
        {"width": 1680, "height": 1050},
        {"width": 3840, "height": 2160},
    ]

    GPUS = [
        {"vendor": "Google Inc. (NVIDIA)", "renderer": "ANGLE (NVIDIA GeForce RTX 3060)"},
        {"vendor": "Google Inc. (NVIDIA)", "renderer": "ANGLE (NVIDIA GeForce GTX 1660)"},
        {"vendor": "Google Inc. (AMD)", "renderer": "ANGLE (AMD Radeon RX 580)"},
        {"vendor": "Google Inc. (Intel)", "renderer": "ANGLE (Intel UHD Graphics 630)"},
        {"vendor": "Google Inc. (Intel)", "renderer": "ANGLE (Intel Iris Xe Graphics)"},
        {"vendor": "Google Inc. (NVIDIA)", "renderer": "ANGLE (NVIDIA GeForce RTX 4070)"},
        {"vendor": "Google Inc. (AMD)", "renderer": "ANGLE (AMD Radeon RX 6700 XT)"},
    ]

    @classmethod
    def _build_user_agent(cls, platform, chrome_version):
        default_ua = (
            f"Mozilla/5.0 ({platform}) AppleWebKit/537.36 "
            f"(KHTML, like Gecko) Chrome/{chrome_version} Safari/537.36"
        )
        if UserAgent is None:
            return default_ua

        try:
            source = str(UserAgent().chrome or "").strip()
            if not source:
                return default_ua
            tail = source.split(")", 1)[1].strip() if ")" in source else ""
            if not tail:
                return default_ua
            tail = re.sub(r"Chrome/\d[\d.]*", f"Chrome/{chrome_version}", tail)
            return f"Mozilla/5.0 ({platform}) {tail}"
        except Exception:
            return default_ua

    @classmethod
    def generate(cls, seed=None):
        """Generate a random but consistent fingerprint payload."""
        if seed is None:
            seed = int(time.time() * 1000) + random.randint(0, 99999)

        rng = random.Random(seed)
        profile = rng.choice(cls.DEVICE_PROFILES)
        chrome_version = rng.choice(cls.CHROME_VERSIONS)
        screen = dict(rng.choice(cls.SCREENS))
        gpu = dict(rng.choice(cls.GPUS))
        languages = list(rng.choice(profile["languages"]))
        timezone = rng.choice(profile["timezones"])
        chrome_major = chrome_version.split(".", 1)[0]
        user_agent = cls._build_user_agent(profile["platform"], chrome_version)

        canvas_seed = rng.randint(1, 999999)
        audio_seed = rng.random() * 0.0001
        webgl_hash = hashlib.md5(
            f"{gpu['vendor']}|{gpu['renderer']}|{seed}".encode("utf-8")
        ).hexdigest()[:8]
        cores = rng.choice([4, 6, 8, 12, 16])
        memory = rng.choice([4, 8, 16, 32])
        plugins_count = rng.randint(3, 7)

        return {
            "user_agent": user_agent,
            "chrome_version": chrome_version,
            "chrome_major": chrome_major,
            "platform": profile["platform"],
            "navigator_platform": profile["navigator_platform"],
            "ua_platform_name": profile["ua_platform_name"],
            "platform_version": profile["platform_version"],
            "screen": screen,
            "gpu": gpu,
            "languages": languages,
            "timezone": timezone,
            "canvas_seed": canvas_seed,
            "audio_seed": audio_seed,
            "webgl_hash": webgl_hash,
            "cores": cores,
            "memory": memory,
            "plugins_count": plugins_count,
            "seed": seed,
        }

    @classmethod
    def build_init_script(cls, fingerprint):
        """Return a Playwright init script that applies the fingerprint to each page."""
        payload = json.dumps(dict(fingerprint or {}), separators=(",", ":"))
        return f"""
(() => {{
    const fp = {payload};

    const defineGetter = (obj, key, value) => {{
        if (!obj) return;
        try {{
            Object.defineProperty(obj, key, {{
                get: () => value,
                configurable: true,
            }});
        }} catch (_error) {{}}
    }};

    const languages = Array.isArray(fp.languages) && fp.languages.length
        ? fp.languages.slice()
        : ['en-US', 'en'];
    const screenInfo = fp.screen || {{ width: 1920, height: 1080 }};

    defineGetter(navigator, 'userAgent', fp.user_agent);
    defineGetter(navigator, 'webdriver', undefined);
    defineGetter(navigator, 'language', languages[0] || 'en-US');
    defineGetter(navigator, 'languages', languages);
    defineGetter(navigator, 'platform', fp.navigator_platform || 'Win32');
    defineGetter(navigator, 'vendor', 'Google Inc.');
    defineGetter(navigator, 'hardwareConcurrency', fp.cores || 8);
    defineGetter(navigator, 'deviceMemory', fp.memory || 8);

    const userAgentData = {{
        brands: [
            {{ brand: 'Chromium', version: String(fp.chrome_major || '131') }},
            {{ brand: 'Google Chrome', version: String(fp.chrome_major || '131') }},
            {{ brand: 'Not=A?Brand', version: '24' }},
        ],
        mobile: false,
        platform: fp.ua_platform_name || 'Windows',
        getHighEntropyValues: async () => ({{
            architecture: 'x86',
            bitness: '64',
            brands: [
                {{ brand: 'Chromium', version: String(fp.chrome_major || '131') }},
                {{ brand: 'Google Chrome', version: String(fp.chrome_major || '131') }},
                {{ brand: 'Not=A?Brand', version: '24' }},
            ],
            fullVersionList: [
                {{ brand: 'Chromium', version: String(fp.chrome_version || '131.0.0.0') }},
                {{ brand: 'Google Chrome', version: String(fp.chrome_version || '131.0.0.0') }},
                {{ brand: 'Not=A?Brand', version: '24.0.0.0' }},
            ],
            mobile: false,
            model: '',
            platform: fp.ua_platform_name || 'Windows',
            platformVersion: fp.platform_version || '10.0.0',
            uaFullVersion: String(fp.chrome_version || '131.0.0.0'),
            wow64: false,
        }}),
        toJSON: () => ({{
            brands: [
                {{ brand: 'Chromium', version: String(fp.chrome_major || '131') }},
                {{ brand: 'Google Chrome', version: String(fp.chrome_major || '131') }},
                {{ brand: 'Not=A?Brand', version: '24' }},
            ],
            mobile: false,
            platform: fp.ua_platform_name || 'Windows',
        }}),
    }};
    defineGetter(navigator, 'userAgentData', userAgentData);

    defineGetter(screen, 'width', Number(screenInfo.width) || 1920);
    defineGetter(screen, 'height', Number(screenInfo.height) || 1080);
    defineGetter(screen, 'availWidth', Number(screenInfo.width) || 1920);
    defineGetter(screen, 'availHeight', Math.max(0, (Number(screenInfo.height) || 1080) - 40));
    defineGetter(screen, 'colorDepth', 24);
    defineGetter(screen, 'pixelDepth', 24);

    const pluginList = Array.from({{ length: Number(fp.plugins_count) || 3 }}, (_value, index) => ({{
        name: 'Plugin ' + index,
        filename: 'plugin' + index + '.dll',
        description: 'Browser Plugin ' + index,
        length: 1,
    }}));
    pluginList.push(
        {{
            name: 'Chrome PDF Plugin',
            filename: 'internal-pdf-viewer',
            description: 'Portable Document Format',
            length: 1,
        }},
        {{
            name: 'Chrome PDF Viewer',
            filename: 'mhjfbmdgcfjbbpaeojofohoefgiehjai',
            description: '',
            length: 1,
        }},
    );
    pluginList.refresh = () => undefined;
    pluginList.item = (index) => pluginList[index] || null;
    pluginList.namedItem = (name) => pluginList.find((entry) => entry.name === name) || null;
    defineGetter(navigator, 'plugins', pluginList);

    const patchWebGL = (Ctor) => {{
        if (!Ctor || !Ctor.prototype || typeof Ctor.prototype.getParameter !== 'function') return;
        const originalGetParameter = Ctor.prototype.getParameter;
        Ctor.prototype.getParameter = function(param) {{
            if (param === 37445) return fp.gpu?.vendor || 'Google Inc. (NVIDIA)';
            if (param === 37446) return fp.gpu?.renderer || 'ANGLE (NVIDIA GeForce RTX 3060)';
            return originalGetParameter.call(this, param);
        }};
    }};
    patchWebGL(window.WebGLRenderingContext);
    patchWebGL(window.WebGL2RenderingContext);

    if (window.HTMLCanvasElement && typeof HTMLCanvasElement.prototype.toDataURL === 'function') {{
        const originalToDataURL = HTMLCanvasElement.prototype.toDataURL;
        HTMLCanvasElement.prototype.toDataURL = function(type) {{
            try {{
                const ctx = this.getContext('2d');
                if (ctx && this.width > 0 && this.height > 0) {{
                    const imageData = ctx.getImageData(0, 0, this.width, this.height);
                    for (let i = 0; i < imageData.data.length; i += 4) {{
                        imageData.data[i] ^= ((Number(fp.canvas_seed) || 1) >> (i % 8)) & 1;
                    }}
                    ctx.putImageData(imageData, 0, 0);
                }}
            }} catch (_error) {{}}
            return originalToDataURL.apply(this, arguments);
        }};
    }}

    const AudioCtor = window.AudioContext || window.webkitAudioContext;
    if (AudioCtor && typeof AudioCtor.prototype.createOscillator === 'function') {{
        const originalCreateOscillator = AudioCtor.prototype.createOscillator;
        AudioCtor.prototype.createOscillator = function() {{
            const osc = originalCreateOscillator.call(this);
            const originalConnect = osc.connect;
            osc.connect = function(dest) {{
                try {{
                    if (window.AnalyserNode && dest instanceof AnalyserNode) {{
                        const gain = this.context.createGain();
                        gain.gain.value = 1 + (Number(fp.audio_seed) || 0);
                        originalConnect.call(this, gain);
                        gain.connect(dest);
                        return dest;
                    }}
                }} catch (_error) {{}}
                return originalConnect.apply(this, arguments);
            }};
            return osc;
        }};
    }}

    window.chrome = Object.assign({{}}, window.chrome || {{}}, {{
        runtime: Object.assign({{}}, (window.chrome && window.chrome.runtime) || {{}}, {{
            connect: function() {{}},
            sendMessage: function() {{}},
        }}),
        loadTimes: function() {{
            const now = Date.now() / 1000;
            return {{
                commitLoadTime: now,
                finishDocumentLoadTime: now + 0.1,
                finishLoadTime: now + 0.2,
            }};
        }},
    }});

    if (navigator.permissions && typeof navigator.permissions.query === 'function') {{
        const originalQuery = navigator.permissions.query.bind(navigator.permissions);
        navigator.permissions.query = function(desc) {{
            if (desc && desc.name === 'notifications') {{
                return Promise.resolve({{ state: 'prompt', onchange: null }});
            }}
            return originalQuery(desc);
        }};
    }}

    const originalResolvedOptions = Intl.DateTimeFormat.prototype.resolvedOptions;
    Intl.DateTimeFormat.prototype.resolvedOptions = function() {{
        const options = originalResolvedOptions.apply(this, arguments);
        return Object.assign({{}}, options, {{
            timeZone: fp.timezone || options.timeZone,
        }});
    }};
}})();
"""
