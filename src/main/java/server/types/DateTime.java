package server.types;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DateTime {
	private int year;
	private int month;
	private int day;
	
	private int hour;
	private int minute;
	private int second;
	private int[] days = { -1, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

	public DateTime(String src) {
		Pattern p = Pattern.compile("^[0-9]{4}/[0-9]{2}/[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$");
		Matcher m = p.matcher(src);

		boolean hasError = false;
		try {
			if (m.find()) {
				int year = Integer.parseInt(src.substring(0, 4));
				int month = Integer.parseInt(src.substring(5, 7));
				int day = Integer.parseInt(src.substring(8, 10));
				
				int hour = Integer.parseInt(src.substring(11, 13));
				int minute = Integer.parseInt(src.substring(14, 16));
				int second = Integer.parseInt(src.substring(17, 19));
				updateDays(year, month, day);
				if (month > 12 || month < 1 || day > days[month] || day < 1 || hour > 23 || hour < 0 || minute > 59 || minute < 0 || second > 59 || second < 0) {
					hasError = true;
				} else {
					this.year = year;
					this.month = month;
					this.day = day;
					
					this.hour = hour;
					this.minute = minute;
					this.second = second;
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
			
			this.hour = -1;
			this.minute = -1;
			this.second = -1;
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
		if (month < 1 || month > 12 || day < 1 || day > days[month] || hour > 23 || hour < 0 || minute > 59 || minute < 0 || second > 59 || second < 0) {
			return false;
		}
		return true;
	}

	public String getDateTime() {
		updateDays();
		if (!isValid()) {
			return null;
		}
		return "" + year + "/" + month + "/" + day + " " + hour + ":" + minute + ":" + second;
	}
}
