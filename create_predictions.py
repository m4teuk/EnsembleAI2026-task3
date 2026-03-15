import argparse

import pandas as pd


T1_MIN = -30.0
T1_MAX = 36.0
T7_MIN = 10.0
T7_MAX = 65.0
A = 0.0065
B = -0.167


def generate_submission(input_csv: str, output_csv: str, a: float = A, b: float = B) -> pd.DataFrame:
	data = pd.read_csv(input_csv)

	if "deviceId" not in data.columns and "device" in data.columns:
		data = data.rename(columns={"device": "deviceId"})

	required_columns = {"deviceId", "timedate", "t1", "t7"}
	missing_columns = required_columns.difference(data.columns)
	if missing_columns:
		missing_list = ", ".join(sorted(missing_columns))
		raise ValueError(f"Missing required columns: {missing_list}")

	if "x2" in data.columns:
		target_scope = data.loc[data["x2"].isna(), ["deviceId", "timedate"]].copy()
	else:
		target_scope = data[["deviceId", "timedate"]].copy()

	target_scope["timedate"] = pd.to_datetime(target_scope["timedate"])
	target_months = (
		target_scope.assign(
			year=target_scope["timedate"].dt.year,
			month=target_scope["timedate"].dt.month,
		)[["deviceId", "year", "month"]]
		.drop_duplicates()
	)

	data = data[["deviceId", "timedate", "t1", "t7"]].copy()
	data["timedate"] = pd.to_datetime(data["timedate"])

	data["t1_real"] = data["t1"] * (T1_MAX - T1_MIN) + T1_MIN
	data["t7_real"] = data["t7"] * (T7_MAX - T7_MIN) + T7_MIN
	data["dhw_lift_delta_t"] = data["t7_real"] - data["t1_real"]

	hourly = (
		data.assign(hour_start=data["timedate"].dt.floor("h"))
		.groupby(["deviceId", "hour_start"], as_index=False)["dhw_lift_delta_t"]
		.mean()
	)
	hourly["dhw_lift_delta_t"] = hourly["dhw_lift_delta_t"].clip(lower=0)

	monthly = (
		hourly.assign(
			year=hourly["hour_start"].dt.year,
			month=hourly["hour_start"].dt.month,
		)
		.groupby(["deviceId", "year", "month"], as_index=False)["dhw_lift_delta_t"]
		.mean()
	)

	monthly["prediction"] = (a * monthly["dhw_lift_delta_t"] + b).clip(0, 1)

	submission = (
		monthly[["deviceId", "year", "month", "prediction"]]
		.merge(target_months, on=["deviceId", "year", "month"], how="inner")
		.sort_values(["deviceId", "year", "month"])
		.reset_index(drop=True)
	)
	submission.to_csv(output_csv, index=False)
	return submission


def main() -> None:
	parser = argparse.ArgumentParser()
	parser.add_argument("--input", default="raw/data_missing_only.csv")
	parser.add_argument("--output", default="submission.csv")
	parser.add_argument("--a", type=float, default=A)
	parser.add_argument("--b", type=float, default=B)
	args = parser.parse_args()

	submission = generate_submission(args.input, args.output, a=args.a, b=args.b)
	print(f"Saved {len(submission)} rows to {args.output}")


if __name__ == "__main__":
	main()
