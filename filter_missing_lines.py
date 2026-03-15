import argparse


def filter_lines_with_missing(input_path: str, output_path: str) -> int:
    kept_rows = 0

    with open(input_path, "r", encoding="utf-8", newline="") as src, open(
        output_path, "w", encoding="utf-8", newline=""
    ) as dst:
        header = src.readline()
        if not header:
            return 0

        dst.write(header)

        for line in src:
            if ",," in line:
                dst.write(line)
                kept_rows += 1

    return kept_rows


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Copy only CSV rows containing ',,' (missing field marker), keeping the header."
    )
    parser.add_argument("--input", default="raw/data.csv")
    parser.add_argument("--output", default="raw/data_missing_only.csv")
    args = parser.parse_args()

    kept_rows = filter_lines_with_missing(args.input, args.output)
    print(f"Saved {kept_rows} data rows to {args.output}")


if __name__ == "__main__":
    main()
