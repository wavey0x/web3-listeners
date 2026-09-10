"""Retired migration-era entry point; no database or network access."""


def recreate_tables():
    raise RuntimeError('Destructive table recreation is retired. Use the versioned server-backup migration tools.')


if __name__ == "__main__":
    recreate_tables()
