BOT_NAME = "nrsr_attendance"

SPIDER_MODULES = ["nrsr_attendance.spiders"]
NEWSPIDER_MODULE = "nrsr_attendance.spiders"

ROBOTSTXT_OBEY = True

USER_AGENT = "nrsr-attendance (+https://github.com/)"

DOWNLOAD_DELAY = 0.5

FEED_EXPORT_ENCODING = "utf-8"

LOG_LEVEL = "INFO"
