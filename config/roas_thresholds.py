'''
    实时播报，roas健康判断标准：
    TikTok全区30%，meta整体要求是40%， meta具体到韩区45% 其他区40%
'''


REALTIME_ROAS_GREEN_THRESHOLDS = {
    "tiktok_all": 0.30,
    "meta_overall": 0.40,
    "meta_kr": 0.45,
    "meta_other": 0.40,
}


def normalize_channel(channel: str) -> str:
    value = (channel or "").strip().lower()
    if value in ("tt", "tiktok", "tik tok"):
        return "tiktok"
    if value in ("meta", "facebook", "fb"):
        return "meta"
    return value


def normalize_country(country: str) -> str:
    value = (country or "").strip().upper()
    if value in ("KR", "KOR", "KOREA", "SOUTH KOREA", "韩国", "韓國", "韩区"):
        return "KR"
    return value


def get_realtime_roas_green_threshold(channel: str, country: str = None) -> float:
    channel = normalize_channel(channel)
    country = normalize_country(country)

    if channel == "tiktok":
        return REALTIME_ROAS_GREEN_THRESHOLDS["tiktok_all"]

    if channel == "meta":
        if country == "KR":
            return REALTIME_ROAS_GREEN_THRESHOLDS["meta_kr"]
        return REALTIME_ROAS_GREEN_THRESHOLDS["meta_other"]

    return REALTIME_ROAS_GREEN_THRESHOLDS["meta_other"]


def evaluate_realtime_roas_green_status(summary, channel_benchmark, meta_country_benchmark):
    '''
        summary:
          实时大盘汇总，至少包含 media_roas、total_spend

        channel_benchmark:
          分渠道大盘，例如：
          {
            "tiktok": {"spend": 1000, "roas": 0.32},
            "facebook": {"spend": 2000, "roas": 0.41}
          }

        meta_country_benchmark:
          Meta 分地区聚合，例如：
          {
            "KR": {"spend": 500, "roas": 0.46},
            "OTHER": {"spend": 1500, "roas": 0.40}
          }
    '''
    checks = []

    tiktok = channel_benchmark.get("tiktok") or channel_benchmark.get("TikTok") or {}
    if tiktok.get("spend", 0) > 0:
        threshold = REALTIME_ROAS_GREEN_THRESHOLDS["tiktok_all"]
        roas = tiktok.get("roas", 0)
        checks.append({
            "name": "TikTok全区",
            "key": "tiktok_all",
            "active": True,
            "roas": roas,
            "threshold": threshold,
            "passed": roas >= threshold,
        })

    meta = (
        channel_benchmark.get("meta")
        or channel_benchmark.get("Meta")
        or channel_benchmark.get("facebook")
        or channel_benchmark.get("Facebook")
        or {}
    )
    if meta.get("spend", 0) > 0:
        threshold = REALTIME_ROAS_GREEN_THRESHOLDS["meta_overall"]
        roas = meta.get("roas", 0)
        checks.append({
            "name": "Meta整体",
            "key": "meta_overall",
            "active": True,
            "roas": roas,
            "threshold": threshold,
            "passed": roas >= threshold,
        })

    meta_kr = meta_country_benchmark.get("KR", {})
    if meta_kr.get("spend", 0) > 0:
        threshold = REALTIME_ROAS_GREEN_THRESHOLDS["meta_kr"]
        roas = meta_kr.get("roas", 0)
        checks.append({
            "name": "Meta韩区",
            "key": "meta_kr",
            "active": True,
            "roas": roas,
            "threshold": threshold,
            "passed": roas >= threshold,
        })

    meta_other = meta_country_benchmark.get("OTHER", {})
    if meta_other.get("spend", 0) > 0:
        threshold = REALTIME_ROAS_GREEN_THRESHOLDS["meta_other"]
        roas = meta_other.get("roas", 0)
        checks.append({
            "name": "Meta其他区",
            "key": "meta_other",
            "active": True,
            "roas": roas,
            "threshold": threshold,
            "passed": roas >= threshold,
        })

    failed_checks = [check for check in checks if not check["passed"]]

    return {
        "is_green": len(checks) > 0 and not failed_checks,#是否绿色
        "checks": checks,#全部
        "failed_checks": failed_checks,#不通过的
    }
