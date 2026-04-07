from corpus import set_source_registry_active, upsert_source_registry
from sources.source_catalog import SOURCE_SEEDS, source_id_for


def seed_sources():
    seeded = upsert_source_registry(SOURCE_SEEDS)
    deactivated = set_source_registry_active([source_id_for("Politico")], False)
    return {"seeded": seeded, "deactivated": deactivated}


if __name__ == "__main__":
    print({"seeded": seed_sources()})
