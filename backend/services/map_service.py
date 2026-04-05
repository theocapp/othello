def get_region_attention_payload(window: str = "24h"):
    from main import _build_region_attention_map
    return _build_region_attention_map(window=window)


def get_hotspot_attention_map_payload(window: str = "24h"):
    from main import _build_hotspot_attention_map
    return _build_hotspot_attention_map(window=window)
