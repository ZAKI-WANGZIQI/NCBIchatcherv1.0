import os
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.keys import Keys
import re
from urllib.parse import quote
import json

# ====== 配置变量 ======
SEARCH_TERMS = [
    "SARS-CoV-2 complete",  # 新型冠状病毒全基因组
]
BASE_DOWNLOAD_DIR = "./ncbi_fasta_downloads"
HEADLESS_MODE = False  # 设置为False可以看到浏览器操作
MAX_PAGES = 285980  # 最大下载页数，防止无限循环
ENABLE_RESUME = True  # 启用断点续传


# =====================

class NCBIFastaDownloader:
    """
    使用Selenium从NCBI下载FASTA蛋白序列（支持多页下载）
    """

    def __init__(self, download_dir=BASE_DOWNLOAD_DIR, headless=HEADLESS_MODE, enable_resume=ENABLE_RESUME):
        """
        初始化下载器

        Args:
            download_dir: 下载目录
            headless: 是否无头模式
            enable_resume: 是否启用断点续传
        """
        self.base_download_dir = os.path.abspath(download_dir)
        self.headless = headless
        self.enable_resume = enable_resume

        # 创建基础下载目录
        if not os.path.exists(self.base_download_dir):
            os.makedirs(self.base_download_dir)
            print(f"创建基础下载目录: {self.base_download_dir}")

        self.driver = None
        self.wait = None
        self.current_download_dir = None
        self.existing_pages = set()  # 记录已存在的页码

    def setup_driver(self, search_term):
        """设置浏览器驱动，并为每个搜索词创建专用下载目录"""
        try:
            # 为搜索词创建安全文件夹名
            safe_folder_name = self._create_safe_folder_name(search_term)
            self.current_download_dir = os.path.join(self.base_download_dir, safe_folder_name)

            # 创建搜索词专用目录
            if not os.path.exists(self.current_download_dir):
                os.makedirs(self.current_download_dir)
                print(f"创建搜索词目录: {self.current_download_dir}")

            # 如果启用断点续传，扫描已存在的文件
            if self.enable_resume:
                self._scan_existing_files(search_term)

            # 明确的ChromeDriver路径
            chromedriver_path = r"C:\Users\wn\.wdm\drivers\chromedriver\win64\141.0.7390.122\chromedriver-win32\chromedriver.exe"

            # 检查路径是否存在
            if not os.path.exists(chromedriver_path):
                print(f"错误: ChromeDriver路径不存在: {chromedriver_path}")
                return False

            print(f"使用ChromeDriver: {chromedriver_path}")

            # 配置选项
            chrome_options = Options()

            if self.headless:
                chrome_options.add_argument("--headless")

            # 设置下载路径为当前搜索词的专用目录
            prefs = {
                "download.default_directory": self.current_download_dir,
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True
            }
            chrome_options.add_experimental_option("prefs", prefs)
            chrome_options.add_argument("--no-sandbox")
            chrome_options.add_argument("--disable-dev-shm-usage")
            chrome_options.add_argument("--disable-gpu")
            chrome_options.add_argument("--window-size=1920,1080")

            # 创建Service对象
            service = Service(executable_path=chromedriver_path)

            print("正在启动浏览器...")
            self.driver = webdriver.Chrome(service=service, options=chrome_options)
            self.wait = WebDriverWait(self.driver, 30)

            print("浏览器驱动设置成功！")
            return True

        except Exception as e:
            print(f"浏览器驱动设置失败: {e}")
            return False

    def _scan_existing_files(self, search_term):
        """扫描已存在的文件，提取页码信息"""
        safe_search_term = self._create_safe_folder_name(search_term)
        pattern = re.compile(rf".*{re.escape(safe_search_term)}_page_(\d+)\.\w+$")

        self.existing_pages.clear()

        if os.path.exists(self.current_download_dir):
            for filename in os.listdir(self.current_download_dir):
                match = pattern.match(filename)
                if match:
                    page_num = int(match.group(1))
                    self.existing_pages.add(page_num)

            print(f"扫描到 {len(self.existing_pages)} 个已下载的页面: {sorted(self.existing_pages)}")

    def _create_safe_folder_name(self, search_term):
        """创建安全的文件夹名称"""
        # 替换特殊字符为下划线
        safe_name = re.sub(r'[<>:"/\\|?*]', '_', search_term)
        # 移除首尾空格和点
        safe_name = safe_name.strip('. ')
        # 限制长度
        if len(safe_name) > 100:
            safe_name = safe_name[:100]
        return safe_name

    def download_fasta_protein(self, search_term):
        """
        下载FASTA蛋白序列（多页版本）

        Args:
            search_term: 搜索词

        Returns:
            下载的文件路径列表
        """
        if not self.driver:
            if not self.setup_driver(search_term):
                return None

        downloaded_files = []
        page_num = 1  # 从第1页开始

        try:
            print(f"开始下载: {search_term}")

            # 步骤1: 访问NCBI搜索页面
            encoded_term = quote(search_term)
            search_url = f"https://www.ncbi.nlm.nih.gov/nuccore/?term={encoded_term}"

            print(f"正在访问: {search_url}")
            self.driver.get(search_url)

            # 等待页面加载
            time.sleep(1.5)
            print("页面加载完成")

            # 获取总页数（如果可能）
            total_pages = self._get_total_pages()
            if total_pages:
                print(f"检测到总页数: {total_pages}")

            while page_num <= MAX_PAGES:
                print(f"\n=== 正在处理第 {page_num} 页 ===")

                # 检查是否已下载过该页（断点续传）
                if self.enable_resume and page_num in self.existing_pages:
                    print(f"第 {page_num} 页已存在，跳过...")
                    # 记录已存在的文件路径
                    safe_search_term = self._create_safe_folder_name(search_term)
                    existing_file = self._find_existing_file(safe_search_term, page_num)
                    if existing_file:
                        downloaded_files.append(existing_file)
                    page_num += 1
                    continue

                # 如果不是第一页且需要跳转，则直接跳转到目标页
                if page_num > 1:
                    if not self._jump_to_page(page_num):
                        print(f"无法跳转到第 {page_num} 页，停止处理")
                        break

                # 记录主窗口句柄
                main_window = self.driver.current_window_handle

                # 步骤2: 选择所有结果
                checkbox_count = self._select_all_checkboxes()
                if checkbox_count == 0:
                    print("当前页没有找到结果，停止处理")
                    break

                print(f"成功选择了 {checkbox_count} 个复选框")

                # 步骤3: 点击Send to按钮
                if not self._click_send_to():
                    print("无法点击Send to按钮")
                    break

                # 步骤4: 选择Coding sequences
                if not self._select_coding_sequences():
                    print("无法选择Coding sequences")
                    break

                # 步骤5: 在新页面选择FASTA protein格式并下载
                downloaded_file = self._configure_and_download(search_term, page_num)

                if downloaded_file:
                    downloaded_files.append(downloaded_file)
                    # 更新已存在的页码
                    if self.enable_resume:
                        self.existing_pages.add(page_num)
                    print(f"第 {page_num} 页下载完成: {downloaded_file}")
                else:
                    print(f"第 {page_num} 页下载失败")

                # 关闭新标签页并切换回主窗口
                self._close_new_tabs_and_return_to_main(main_window)

                # 重要：取消勾选所有复选框，避免重复选择
                self._deselect_all_checkboxes()

                page_num += 1
                time.sleep(1.5)  # 等待页面加载

            print(f"\n{search_term} 共处理了 {page_num - 1} 页，成功下载 {len(downloaded_files)} 个文件")
            return downloaded_files

        except Exception as e:
            print(f"下载过程中出错: {e}")
            return downloaded_files

    def _find_existing_file(self, safe_search_term, page_num):
        """查找已存在的文件"""
        pattern = re.compile(rf".*{re.escape(safe_search_term)}_page_{page_num}\.\w+$")

        if os.path.exists(self.current_download_dir):
            for filename in os.listdir(self.current_download_dir):
                if pattern.match(filename):
                    return os.path.join(self.current_download_dir, filename)
        return None

    def _get_total_pages(self):
        """获取总页数"""
        try:
            # 查找页码输入框，其中的last属性包含总页数
            page_input = self.driver.find_element(
                By.CSS_SELECTOR,
                "input[name='EntrezSystem2.PEntrez.Nuccore.Sequence_ResultsPanel.Entrez_Pager.cPage']"
            )
            total_pages = page_input.get_attribute("last")
            if total_pages:
                return int(total_pages)
        except Exception as e:
            print(f"获取总页数失败: {e}")
        return None

    def _jump_to_page(self, target_page):
        """直接跳转到指定页码"""
        try:
            print(f"正在跳转到第 {target_page} 页...")

            # 方法1: 通过页码输入框直接跳转
            try:
                # 定位页码输入框
                page_input = self.driver.find_element(
                    By.CSS_SELECTOR,
                    "input[name='EntrezSystem2.PEntrez.Nuccore.Sequence_ResultsPanel.Entrez_Pager.cPage']"
                )

                # 清空输入框并输入目标页码
                page_input.clear()
                page_input.send_keys(str(target_page))
                page_input.send_keys(Keys.RETURN)  # 按回车键提交

                print(f"已通过输入框跳转到第 {target_page} 页")
                time.sleep(2)  # 等待页面加载
                return True

            except Exception as e:
                print(f"通过输入框跳转失败: {e}")

            # 方法2: 通过Next按钮的page属性直接构造点击
            try:
                # 查找Next按钮，通过page属性判断
                next_buttons = self.driver.find_elements(
                    By.CSS_SELECTOR,
                    "a.page_link.next"
                )

                for button in next_buttons:
                    page_attr = button.get_attribute("page")
                    if page_attr and int(page_attr) == target_page:
                        button.click()
                        print(f"已通过Next按钮跳转到第 {target_page} 页")
                        time.sleep(2)  # 等待页面加载
                        return True

            except Exception as e:
                print(f"通过Next按钮跳转失败: {e}")

            # 方法3: 使用您提供的XPATH
            try:
                next_button = self.driver.find_element(
                    By.XPATH,
                    "/html/body/div[1]/div[1]/form/div[1]/div[4]/div/div[7]/div/a[3]"
                )
                next_button.click()
                print(f"已通过XPATH跳转到第 {target_page} 页")
                time.sleep(2)  # 等待页面加载
                return True
            except Exception as e:
                print(f"通过XPATH跳转失败: {e}")

            print("所有跳转方式都失败了")
            return False

        except Exception as e:
            print(f"跳转到第 {target_page} 页时出错: {e}")
            return False

    def _select_all_checkboxes(self):
        """选择所有复选框 - 返回选中的复选框数量"""
        try:
            # 等待页面加载
            time.sleep(1.5)

            # 查找所有符合条件的复选框
            checkboxes = self.driver.find_elements(
                By.CSS_SELECTOR,
                "input[name='EntrezSystem2.PEntrez.Nuccore.Sequence_ResultsPanel.Sequence_RVDocSum.uid']"
            )

            if not checkboxes:
                print("未找到任何复选框")
                return 0

            print(f"找到 {len(checkboxes)} 个复选框")

            # 选中所有复选框
            selected_count = 0
            for i, checkbox in enumerate(checkboxes):
                try:
                    if not checkbox.is_selected():
                        # 使用JavaScript点击，更可靠
                        self.driver.execute_script("arguments[0].click();", checkbox)
                        selected_count += 1
                    print(f"已选择复选框 {i + 1}")
                except Exception as e:
                    print(f"选择复选框 {i + 1} 时出错: {e}")

            print(f"已成功选择 {selected_count} 个复选框")
            time.sleep(1)
            return selected_count

        except Exception as e:
            print(f"选择复选框时出错: {e}")
            return 0

    def _deselect_all_checkboxes(self):
        """取消勾选所有复选框 - 防止重复选择"""
        try:
            # 等待页面加载
            time.sleep(1)

            # 查找所有符合条件的复选框
            checkboxes = self.driver.find_elements(
                By.CSS_SELECTOR,
                "input[name='EntrezSystem2.PEntrez.Nuccore.Sequence_ResultsPanel.Sequence_RVDocSum.uid']"
            )

            if not checkboxes:
                print("未找到任何复选框用于取消选择")
                return 0

            print(f"找到 {len(checkboxes)} 个复选框用于取消选择")

            # 取消选中所有复选框
            deselected_count = 0
            for i, checkbox in enumerate(checkboxes):
                try:
                    if checkbox.is_selected():
                        # 使用JavaScript点击，更可靠
                        self.driver.execute_script("arguments[0].click();", checkbox)
                        deselected_count += 1
                    print(f"已取消选择复选框 {i + 1}")
                except Exception as e:
                    print(f"取消选择复选框 {i + 1} 时出错: {e}")

            print(f"已成功取消选择 {deselected_count} 个复选框")
            time.sleep(1)
            return deselected_count

        except Exception as e:
            print(f"取消选择复选框时出错: {e}")
            return 0

    def _click_send_to(self):
        """点击Send to按钮"""
        try:
            # 方法1: 使用您提供的确切XPATH
            print("尝试使用提供的XPATH点击Send to按钮...")
            send_to_button = self.wait.until(
                EC.element_to_be_clickable((By.XPATH, "/html/body/div[1]/div[1]/form/div[1]/div[4]/div/div[1]/h4/a"))
            )
            send_to_button.click()
            print("已通过XPATH点击Send to按钮")
            time.sleep(1)
            return True

        except TimeoutException:
            print("XPATH方式失败，尝试其他定位方式...")

            # 方法2: 通过ID查找
            try:
                send_to_button = self.driver.find_element(By.ID, "sendto")
                send_to_button.click()
                print("已通过ID点击Send to按钮")
                time.sleep(1)
                return True
            except:
                pass

            # 方法3: 通过链接文本查找
            try:
                send_to_buttons = self.driver.find_elements(By.LINK_TEXT, "Send to")
                if send_to_buttons:
                    send_to_buttons[0].click()
                    print("已通过链接文本点击Send to按钮")
                    time.sleep(1)
                    return True
            except:
                pass

            print("所有Send to按钮定位方式都失败了")
            return False

    def _select_coding_sequences(self):
        """选择Coding sequences"""
        try:
            # 等待菜单加载
            time.sleep(1)

            # 方法1: 使用您提供的确切XPATH
            print("尝试使用提供的XPATH选择Coding sequences...")
            coding_seq_option = self.wait.until(
                EC.element_to_be_clickable(
                    (By.XPATH, "/html/body/div[1]/div[1]/form/div[1]/div[4]/div/div[1]/div[4]/fieldset/ul/li[2]/input"))
            )
            coding_seq_option.click()
            print("已通过XPATH选择Coding sequences")
            time.sleep(1)  # 等待页面跳转
            return True

        except TimeoutException:
            print("XPATH方式失败，尝试其他定位方式...")

            # 方法2: 通过链接文本查找
            try:
                coding_seq_option = self.wait.until(
                    EC.element_to_be_clickable((By.LINK_TEXT, "Coding sequences"))
                )
                coding_seq_option.click()
                print("已通过链接文本选择Coding sequences")
                time.sleep(2)
                return True
            except:
                pass

            # 方法3: 通过部分链接文本查找
            try:
                coding_seq_options = self.driver.find_elements(By.PARTIAL_LINK_TEXT, "Coding")
                for option in coding_seq_options:
                    if "Coding sequences" in option.text:
                        option.click()
                        print("已通过部分链接文本选择Coding sequences")
                        time.sleep(1.5)
                        return True
            except:
                pass

            print("所有Coding sequences定位方式都失败了")
            return False

    def _configure_and_download(self, search_term, page_num):
        """在新页面配置并下载文件"""
        try:
            # 等待新页面加载
            time.sleep(1.5)

            # 查找格式选择下拉菜单
            print("正在查找格式选择菜单...")
            format_select = self.wait.until(
                EC.presence_of_element_located((By.ID, "codeseq_format"))
            )

            # 选择FASTA Protein格式
            select = Select(format_select)
            select.select_by_value("fasta_cds_aa")  # 这是正确的value
            print("已选择FASTA Protein格式")
            time.sleep(1)

            # 点击创建文件按钮
            print("正在点击创建文件按钮...")
            create_button = self.wait.until(
                EC.element_to_be_clickable(
                    (By.XPATH, "/html/body/div[1]/div[1]/form/div[1]/div[4]/div/div[1]/div[4]/div[2]/button"))
            )
            create_button.click()
            print("已通过XPATH点击创建文件按钮")

            # 等待下载完成
            downloaded_file = self._wait_for_download(search_term, page_num, timeout=210)
            return downloaded_file

        except TimeoutException:
            print("XPATH方式失败，尝试其他定位方式...")

            # 备选方案：尝试其他方式定位创建文件按钮
            create_selectors = [
                "//button[contains(text(), 'Create File')]",
                "//button[contains(text(), 'Create')]",
                "//input[@type='submit' and contains(@value, 'Create File')]",
                "//input[@type='submit' and contains(@value, 'Create')]",
                "//input[@name='CreateFile']"
            ]

            for selector in create_selectors:
                try:
                    create_button = self.driver.find_element(By.XPATH, selector)
                    if create_button.is_displayed() and create_button.is_enabled():
                        create_button.click()
                        print(f"已点击创建文件按钮 (使用选择器: {selector})")

                        # 等待下载完成
                        downloaded_file = self._wait_for_download(search_term, page_num, timeout=120)
                        return downloaded_file
                except:
                    continue

            print("所有创建文件按钮定位方式都失败了")
            return None

        except Exception as e:
            print(f"配置下载时出错: {e}")
            return None

    def _wait_for_download(self, search_term, page_num, timeout=120):
        """等待下载完成，并重命名文件"""
        print(f"等待下载完成，最多等待 {timeout} 秒...")

        initial_files = set(os.listdir(self.current_download_dir))
        start_time = time.time()

        while time.time() - start_time < timeout:
            current_files = set(os.listdir(self.current_download_dir))
            new_files = current_files - initial_files

            # 检查是否有新的下载文件
            for file in new_files:
                file_lower = file.lower()
                # 检查常见下载文件扩展名
                if any(ext in file_lower for ext in ['.fasta', '.fa', '.txt']):
                    # 排除临时文件
                    if not file.endswith('.crdownload') and not file.endswith('.tmp'):
                        file_path = os.path.join(self.current_download_dir, file)

                        # 重命名文件以包含搜索词和页码
                        file_ext = os.path.splitext(file)[1]
                        safe_search_term = self._create_safe_folder_name(search_term)
                        new_filename = f"{safe_search_term}_page_{page_num}{file_ext}"
                        new_file_path = os.path.join(self.current_download_dir, new_filename)

                        # 如果目标文件已存在，则删除（理论上不应该存在，因为启用了断点续传）
                        if os.path.exists(new_file_path):
                            os.remove(new_file_path)

                        os.rename(file_path, new_file_path)
                        print(f"下载完成! 文件已重命名为: {new_filename}")
                        return new_file_path

            # 检查是否还有.crdownload临时文件
            temp_files = [f for f in current_files if f.endswith('.crdownload')]
            if temp_files:
                print(f"下载中... ({len(temp_files)} 个临时文件)")

            time.sleep(2)

        print("下载超时")
        return None

    def _close_new_tabs_and_return_to_main(self, main_window):
        """关闭新标签页并返回主窗口"""
        try:
            # 获取所有窗口句柄
            all_windows = self.driver.window_handles

            # 关闭所有非主窗口
            for window in all_windows:
                if window != main_window:
                    self.driver.switch_to.window(window)
                    self.driver.close()

            # 切换回主窗口
            self.driver.switch_to.window(main_window)
            print("已关闭新标签页并返回主窗口")

        except Exception as e:
            print(f"关闭标签页时出错: {e}")

    def close(self):
        """关闭浏览器"""
        if self.driver:
            self.driver.quit()
            print("浏览器已关闭")


def main():
    """
    主函数 - 运行这个来下载FASTA文件
    """
    print("=" * 60)
    print("NCBI FASTA蛋白序列下载器（多页多搜索词版本）")
    print("=" * 60)
    print(f"搜索词: {SEARCH_TERMS}")
    print(f"基础下载目录: {BASE_DOWNLOAD_DIR}")
    print(f"最大页数: {MAX_PAGES}")
    print(f"断点续传: {'启用' if ENABLE_RESUME else '禁用'}")
    print("=" * 60)

    # 创建基础下载目录
    if not os.path.exists(BASE_DOWNLOAD_DIR):
        os.makedirs(BASE_DOWNLOAD_DIR)

    all_downloaded_files = {}

    try:
        for search_term in SEARCH_TERMS:
            print(f"\n开始处理: {search_term}")
            print("-" * 40)

            # 为每个搜索词创建新的下载器（这样每个搜索词都有独立的下载目录）
            downloader = NCBIFastaDownloader(
                download_dir=BASE_DOWNLOAD_DIR,
                headless=HEADLESS_MODE,
                enable_resume=ENABLE_RESUME
            )

            try:
                # 下载FASTA蛋白序列（多页）
                downloaded_files = downloader.download_fasta_protein(search_term)

                if downloaded_files:
                    all_downloaded_files[search_term] = downloaded_files
                    print(f"\n下载成功! 共下载 {len(downloaded_files)} 个文件:")
                    for file_path in downloaded_files:
                        print(f"  - {os.path.basename(file_path)}")
                else:
                    print(f"下载失败，请检查网络连接或搜索词")

                print(f"\n{search_term} 处理完成")

            except Exception as e:
                print(f"处理 {search_term} 时出错: {e}")

            finally:
                # 关闭当前搜索词的浏览器
                downloader.close()
                time.sleep(2)  # 间隔时间

    except Exception as e:
        print(f"程序运行出错: {e}")

    finally:
        print("\n程序运行结束")

        # 打印总结
        if all_downloaded_files:
            print("\n" + "=" * 60)
            print("下载总结:")
            print("=" * 60)
            total_files = 0
            for search_term, files in all_downloaded_files.items():
                print(f"{search_term}: {len(files)} 个文件")
                total_files += len(files)
            print(f"总计: {total_files} 个文件")
        else:
            print("没有成功下载任何文件")


if __name__ == "__main__":
    main()