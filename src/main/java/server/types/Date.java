package server.types;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Date {

	private int year;
	private int month;
	private int day;
	private int[] days = { -1, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

	public Date(String src) {
		Pattern p = Pattern.compile("^[0-9]{4}/[0-9]{2}/[0-9]{2}$");
		Matcher m = p.matcher(src);

		boolean hasError = false;
		try {
			if (m.find()) {
				int year = Integer.parseInt(src.substring(0, 4));
				int month = Integer.parseInt(src.substring(5, 7));
				int day = Integer.parseInt(src.substring(8, 10));
				updateDays(year, month, day);
				if (month > 12 || month < 1 || day > days[month] || day < 1) {
					hasError = true;
				} else {
					this.year = year;
					this.month = month;
					this.day = day;
				}
			} else {
				hasError = true;
			}
		} catch (NumberFormatException e) {
			hasError = true;
		}
		if (hasError) {
			this.year = -1;
			this.month = -1;
			this.day = -1;
		}
	}

	private void updateDays() {
		if ((year % 4 == 0 && year % 100 != 0) || year % 400 == 0) {
			days[2] = 29;
		} else {
			days[2] = 28;
		}
	}

	private void updateDays(int year, int month, int date) {
		if ((year % 4 == 0 && year % 100 != 0) || year % 400 == 0) {
			days[2] = 29;
		} else {
			days[2] = 28;
		}
	}
	

	public boolean isValid() {
		updateDays();
		if (year == -1 || month < 1 || month > 12 || day < 1 || day > days[month]) {
			return false;
		}
		return true;
	}

	public String getDate() {
		updateDays();
		if (!isValid()) {
			return null;
		}
		return "" + year + "/" + month + "/" + day;
	}
	
}
