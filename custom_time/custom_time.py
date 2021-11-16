from datetime import timezone, timedelta
from dateutil import tz
import pytz
import datetime

class CustomTimeZone:
    MACHINE_TIME_ZONE = ""
    STOCK_MARKET_LOCATION = "America/New_York"

    def __init__(self, time_zone="America/New_York"):
        """
        if time_zone == MACHINE_TIME_ZONE or "", then the current machine timezone will be considered
        :param time_zone:
        """
        self.time_zone = time_zone
        if (self.time_zone not in pytz.common_timezones) and (self.time_zone != self.MACHINE_TIME_ZONE):
            raise Exception(f"Wrong time zone : ({self.time_zone}) Valid time zones are: \n {pytz.common_timezones}")

    def get_current_utc_iso_date(self):
        utc_now = pytz.utc.localize(datetime.datetime.utcnow())
        return utc_now.date().isoformat()

    def get_current_utc_iso_time(self):
        utc_now = pytz.utc.localize(datetime.datetime.utcnow())
        return utc_now.time().strftime("%H:%M:%S")

    def get_current_utc_iso_time_date_tuple(self):
        utc_now = pytz.utc.localize(datetime.datetime.utcnow())
        return utc_now.time().strftime("%H:%M:%S"), utc_now.date().isoformat()

    def get_utc_stamp(self, y, m, d, h, mi, s, plusday=0):
        date = datetime.datetime(y, m, d, h, mi, s) + timedelta(days=plusday)
        timestamp = date.replace(tzinfo=timezone.utc)
        return int(timestamp.timestamp())

    def get_n_min_prev_unix(self, nano_unix, n):
        """
        Reduce n minutes from given nano_unix and return the calculated nano_unix
        :param nano_unix : the base timestamp in nano second
        :param n : number of minutes that needs to be reduced from nano_unix
        :returns n minutes previous timestamp based on nano_unix param
        """
        return nano_unix - (n * 60000000000)

    def get_n_min_next_unix(self, nano_unix, n):
        """
        Add n minutes with given nano_unix and return the calculated nano_unix
        :param nano_unix : the base timestamp in nano second
        :param n : number of minutes that needs to be added with nano_unix
        :returns: n minutes next timestamp based on nano_unix param
        """
        return nano_unix + (n * 60000000000)

    def get_current_unix_time(self):
        """
        Current UTC/UNIX timestamp in nanosecond. Python datetime api returns timestamp
        in second format as float.
        :return: UNIX timestamp in nano (lossy nano second is not precise)
        """
        return int(datetime.datetime.now().timestamp() * 1000000000)

    def get_current_unix_time_in_mili(self):
        return int(datetime.datetime.now().timestamp() * 1000)

    def reduce_n_day_from_iso_date(self, n, iso_date):
        """
        Subtract n day from given iso_date. It is considered that all params are valid

        :param n: total days to be subtracted
        :param iso_date: iso date from which the days will be subtracted
        :return: iso date as string
        """
        d_parts = datetime.datetime.fromisoformat(iso_date)
        reduced_date = datetime.date(d_parts.year, d_parts.month, d_parts.day) - datetime.timedelta(n)
        return str(reduced_date)

    def get_date_time_from_nano_timestamp(self, nano_unix):
        """
        Datetime is generated for specified time zone only
        :param nano_unix:
        :return:
        """
        from_zone = tz.tzutc()
        if self.time_zone != self.MACHINE_TIME_ZONE:
            to_zone = tz.gettz(self.time_zone)
        else:
            to_zone = tz.tzlocal()

        return datetime.datetime.fromtimestamp(nano_unix / 1000000000, tz=to_zone)

    def get_date_time_from_mili_timestamp(self, mili_unix):
        """
        Datetime is generated for specified time zone only
        :param mili_unix:
        :return:
        """
        from_zone = tz.tzutc()
        if self.time_zone != self.MACHINE_TIME_ZONE:
            to_zone = tz.gettz(self.time_zone)
        else:
            to_zone = tz.tzlocal()

        return datetime.datetime.fromtimestamp(mili_unix / 1000, tz=to_zone)

    def get_tz_date_from_nano_unix(self, nano_unix):
        date_time = self.get_date_time_from_nano_timestamp(nano_unix)
        return date_time.date().isoformat()

    def get_tz_time_from_nano_unix(self, nano_unix):
        date_time = self.get_date_time_from_nano_timestamp(nano_unix)
        return date_time.time().isoformat()

    def get_tz_time_date_tuple_from_nano_unix(self, nano_unix, strf="%H:%M:%S"):
        date_time = self.get_date_time_from_nano_timestamp(nano_unix)
        return date_time.time().strftime(strf), date_time.date().isoformat()

    def get_strf_tz_time_from_nano_unix(self, nano_unix, strf="%H:%M:%S"):
        date_time = self.get_date_time_from_nano_timestamp(nano_unix)
        return date_time.time().strftime(strf)

    def get_current_iso_date(self):
        """
        Calculates current date of a specific timezone
        if the timezone is "" or empty string then current machines current date is returned

        :raises Exception when given time zone is invalid:
        :returns: iso date
        """
        if self.time_zone == self.MACHINE_TIME_ZONE:
            # return current date of machine
            return datetime.datetime.now().strftime("%Y-%m-%d")
        else:
            # Time zone is valid
            utc_now = pytz.utc.localize(datetime.datetime.utcnow())

            tz_time_now = utc_now.astimezone(pytz.timezone(self.time_zone))
            return tz_time_now.date().isoformat()

    def get_current_iso_time_date_tuple(self):
        """
        Calculates current date of a specific timezone
        if the timezone is "" or empty string then current machines current date is returned

        :raises Exception when given time zone is invalid:
        :returns: iso date
        """
        if self.time_zone == self.MACHINE_TIME_ZONE:
            # return current date of machine
            curr_time_date = datetime.datetime.now()

            return curr_time_date.time().strftime("%H:%M:%S"), curr_time_date.date().strftime("%Y-%m-%d")
        else:
            # Time zone is valid
            utc_now = pytz.utc.localize(datetime.datetime.utcnow())

            tz_time_now = utc_now.astimezone(pytz.timezone(self.time_zone))
            return tz_time_now.time().strftime("%H:%M:%S"), tz_time_now.date().strftime("%Y-%m-%d")


if __name__ == "__main__":
    cus_time = CustomTimeZone()
    print(cus_time.get_date_time_from_mili_timestamp(1622922755))
