# install_chromedriver.py
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager

# 自动下载并安装ChromeDriver
driver_path = ChromeDriverManager().install()
print(f"ChromeDriver已安装到: {driver_path}")