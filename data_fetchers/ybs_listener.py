"""Retired migration-era entry point; no database or network access."""


def main():
    raise RuntimeError('The duplicate YBS listener is retired. Use Open Data Scripts for YBS indexing.')


if __name__ == "__main__":
    main()
