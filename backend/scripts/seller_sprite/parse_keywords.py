import os
import sys

import re
import json
from lxml import etree
import requests
from backend.app.core.mongo_client import get_database

class KeywordRowParser:
    """
    解析关键词数据行的解析器
    包含17个方法，分别对应17个td的解析逻辑
    """
    def parse(self, row):
        tds = row.xpath("./td")
        if len(tds) != 17:
            print(f"Warning: Expected 17 tds, found {len(tds)}")
            # 暂时返回空，或者尝试解析部分
            if len(tds) < 17:
                return {}
        
        item = {}
        # 依次调用17个解析方法
        # 注意：这里假设 tds 下标 0-16 对应 17 个字段
        item.update(self.parse_td_0(tds[0]))
        # item.update(self.parse_td_1(tds[1]))
        item.update(self.parse_td_2(tds[2]))
        item.update(self.parse_td_3(tds[2]))
        item.update(self.parse_td_4(tds[4]))
        item.update(self.parse_td_5(tds[5]))
        item.update(self.parse_td_6(tds[6]))
        item.update(self.parse_td_7(tds[7]))
        item.update(self.parse_td_8(tds[8]))
        item.update(self.parse_td_9(tds[9]))
        item.update(self.parse_td_10(tds[10]))
        item.update(self.parse_td_11(tds[11]))
        item.update(self.parse_td_12(tds[12]))
        item.update(self.parse_td_13(tds[13]))
        item.update(self.parse_td_14(tds[14]))
        item.update(self.parse_td_15(tds[15]))
        item.update(self.parse_td_16(tds[16]))
        
        return item

    def parse_td_0(self, td):
        """
        解析第1列: 复选框 (Checkbox)
        通常包含 data-keyword 属性，可用于提取关键词ID
        """
        # Checkbox column - extract keyword ID or similar if needed
        # <input type="checkbox" ... data-keyword="jellycat plush toy">
        return {}

    def parse_td_1(self, td):
        """
        解析第2列: 排名 (Rank)
        每次都会变动，没有意义，爬取时忽略这个
        """
        # Rank: <p class="text-muted text-center mb-0">1</p>
        # try:
        #     text = td.xpath(".//p/text()")
        #     if text:
        #         return {'rank': int(text[0].strip())}
        # except:
        #     pass
        # return {'rank': 0}
        pass

    def parse_td_2(self, td):
        """
        解析第3列: 关键词信息 (Keyword Info)
        包含英文关键词和中文翻译
        """
        # Keyword Info
        item = {}
        try:
            # English Keyword
            # <a class="link-dark lighter" ...>jellycat plush toy</a>
            kw_node = td.xpath(".//a[contains(@class, 'link-dark')]")
            if kw_node:
                item['name'] = kw_node[0].text.strip()
            
            # CN Translation
            # <span class="span-keywords keyword-cn-color ...">jellycat 毛绒玩具</span>
            cn_node = td.xpath(".//span[contains(@class, 'keyword-cn-color')]")
            if cn_node:
                item['name_cn'] = cn_node[0].text.strip()
        except:
            pass
        return item

    def parse_td_3(self, td):
        """
        解析第4列: Top商品图 (Top Products)
        实际上使用的是第3列(tds[2])的数据，因为其中包含完整的 Popover 商品列表 (10个)
        """
        item = {}
        try:
            # Top 10 Products from Popover
            # <div class="popover-item">
            products = []
            pop_items = td.xpath(".//div[contains(@class, 'popover-item')]")
            for pop in pop_items:
                prod = {}
                # Amazon URL
                a_node = pop.xpath(".//a[contains(@class, 'bg-white')]")
                if a_node:
                    prod['url'] = a_node[0].get('href')
                
                # Image URL
                img_node = pop.xpath(".//div[contains(@class, 'popover-img')]")
                if img_node:
                    style = img_node[0].get('style')
                    if style:
                        m = re.search(r"url\(['\"]?(.*?)['\"]?\)", style)
                        if m:
                            prod['image'] = m.group(1)
                
                # Title
                title_node = pop.xpath(".//h4")
                if title_node:
                    prod['title'] = title_node[0].text.strip()
                
                # Price
                price_node = pop.xpath(".//p[contains(@class, 'price-box')]")
                if price_node:
                    prod['price'] = price_node[0].text.strip()
                
                # Rating & Count: "7,583(4.8)"
                rating_node = pop.xpath(".//div[contains(@class, 'price-rating-des')]/p[contains(@class, 'text-primary')][last()]")
                if rating_node:
                    rating_text = rating_node[0].text.strip()
                    # Parse "7,583(4.8)"
                    m_rating = re.search(r"([\d,]+)\s*\(([\d\.]+)\)", rating_text)
                    if m_rating:
                        prod['rating_count'] = int(m_rating.group(1).replace(',', ''))
                        prod['rating'] = float(m_rating.group(2))
                    else:
                        # Fallback if format differs
                        prod['rating_info'] = rating_text
                
                products.append(prod)
            
            item['top_products_details'] = products
            # item['top_products'] = [p['image'] for p in products if 'image' in p]

        except Exception as e:
            print(f"Error parsing td_3 (products): {e}")
        return item

    def parse_td_4(self, td):
        """
        解析第5列: 趋势数据 (Trend Chart)
        解析 data-y 属性中的 JSON 数据
        """
        # Trend Chart
        # <div class="morris-table-inline" data-y="...">
        item = {'trend_data': []}
        try:
            nodes = td.xpath(".//div[contains(@class, 'morris-table-inline')]")
            if nodes:
                data_y = nodes[0].get('data-y')
                if data_y:
                    json_str = data_y.replace("'", '"')
                    trend_data = json.loads(json_str)
                    formatted = []
                    for point in trend_data:
                        date_str = str(point['x'])
                        if len(date_str) == 6:
                            date_str = f"{date_str[:4]}-{date_str[4:]}"
                        formatted.append({
                            'date': date_str,
                            'value': point['y']
                        })
                    item['trend_data'] = formatted
        except:
            pass
        return item

    def parse_td_5(self, td):
        """
        解析第6列: 搜索量 (Search Volume)
        """
        # Search Volume
        # <div class="pr-4">60,265</div>
        item = {'search_volume': 0}
        try:
            divs = td.xpath("./div")
            if divs:
                text = divs[0].text
                if text:
                    item['search_volume'] = int(text.replace(',', '').strip())
        except:
            pass
        return item

    def parse_td_6(self, td):
        """
        解析第7列: 购买量 / 购买率 (Purchase Volume / Rate)
        """
        # Purchase Volume / Rate
        # <div class="pr-4">621</div>
        # <div class="pr-4 text-muted">1.03%</div>
        item = {'purchase_volume': 0, 'purchase_rate': 0.0}
        try:
            divs = td.xpath("./div")
            if len(divs) >= 1:
                text = divs[0].text
                if text:
                    item['purchase_volume'] = int(text.replace(',', '').strip())
            if len(divs) >= 2:
                text = divs[1].text
                if text:
                    item['purchase_rate'] = float(text.replace('%', '').strip()) / 100
        except:
            pass
        return item

    def parse_td_7(self, td):
        """
        解析第8列: 展示量 / 点击量 (Impressions / Clicks)
        """
        # Impressions / Clicks (Assuming based on structure)
        # <div class="pr-4">631,462</div>
        # <div class="pr-4">25,282</div>
        item = {'impressions': 0, 'clicks': 0}
        try:
            divs = td.xpath("./div")
            if len(divs) >= 1:
                text = divs[0].text
                if text:
                    item['impressions'] = int(text.replace(',', '').strip())
            if len(divs) >= 2:
                text = divs[1].text
                if text:
                    item['clicks'] = int(text.replace(',', '').strip())
        except:
            pass
        return item

    def parse_td_8(self, td):
        """
        解析第9列: 增长率 (Growth Rate)
        """
        # Growth Rate
        # <div class="pr-2">1.00%</div>
        item = {'growth_rate': 0.0}
        try:
            divs = td.xpath("./div")
            if divs:
                text = divs[0].text
                if text:
                    item['growth_rate'] = float(text.replace('%', '').strip()) / 100
        except:
            pass
        return item

    def parse_td_9(self, td):
        """
        解析第10列: 同比增长 (YoY Growth)
        """
        # YoY Growth
        # <div>60,265 (1.00%)</div>
        item = {'yoy_growth': 0, 'yoy_growth_rate': 0.0}
        try:
            divs = td.xpath("./div")
            if divs:
                text = "".join(divs[0].xpath(".//text()")).strip()
                m = re.search(r"([\d,]+)\s*\(([\d\.\-%]+)\)", text)
                if m:
                    item['yoy_growth'] = int(m.group(1).replace(',', ''))
                    item['yoy_growth_rate'] = float(m.group(2).replace('%', '')) / 100
        except:
            pass
        return item

    def parse_td_10(self, td):
        """
        解析第11列: 点击集中度 / 垄断系数 (Click Share / Monopoly)
        """
        # Click Share / Monopoly
        # <a ...>17.03%</a>
        # <div class="text-muted">19.95%</div>
        item = {'click_share': 0.0, 'total_share': 0.0}
        try:
            a_node = td.xpath(".//a")
            if a_node:
                text = a_node[0].text
                if text:
                    item['click_share'] = float(text.replace('%', '').strip()) / 100
            
            div_node = td.xpath(".//div[contains(@class, 'text-muted')]")
            if div_node:
                text = div_node[0].text
                if text:
                    item['total_share'] = float(text.replace('%', '').strip()) / 100
        except:
            pass
        return item

    def parse_td_11(self, td):
        """
        解析第12列: 转化份额 (Conversion Share)
        """
        # Conversion Share?
        # <div class="pr-4">2.5%</div>
        item = {'conversion_share': 0.0}
        try:
            divs = td.xpath("./div")
            if divs:
                text = divs[0].text
                if text:
                    item['conversion_share'] = float(text.replace('%', '').strip()) / 100
        except:
            pass
        return item

    def parse_td_12(self, td):
        """
        解析第13列: PPC竞价 (PPC Bid)
        包含 Min, Avg, Max
        """
        # PPC Bid
        # <div ... ppc-min="">$0.57</div>
        # <a ...>$0.76</a>
        # <div ... ppc-max="">$1.07</div>
        item = {'ppc_bid': 0.0, 'ppc_min': 0.0, 'ppc_max': 0.0}

        min_node = td.xpath(".//div[@ppc-min]")

        if min_node:
            text = min_node[0].text
            if text:
                item['ppc_min'] = float(text.replace('$', '').strip())


        avg_node = td.xpath(".//a[@pop-type='cpc_bid']")
        if avg_node:
            text = avg_node[0].text
            if text:

                item['ppc_bid'] = float(text.replace('$', '').strip())

        max_node = td.xpath(".//div[@ppc-max]")
        if max_node:
            text = max_node[0].text
            if text:

                item['ppc_max'] = float(text.replace('$', '').strip())


        return item

    def parse_td_13(self, td):
        """
        解析第14列: 隐藏列 (Hidden)
        通常包含旧版数据或辅助信息
        """
        # Hidden column
        return {}

    def parse_td_14(self, td):
        """
        解析第15列: 需供比 / 商品数 (Supply/Demand Ratio & Product Count)
        """
        # Supply/Demand Ratio & Product Count
        # <div><span class="pr-2">147.0</span></div>
        # <div><span class="text-muted pr-2">410</span></div>
        item = {'supply_demand_ratio': 0.0, 'product_count': 0}
        try:
            divs = td.xpath("./div")
            if len(divs) >= 1:
                text = "".join(divs[0].xpath(".//text()")).strip()
                if text:
                    item['supply_demand_ratio'] = float(text)
            if len(divs) >= 2:
                text = "".join(divs[1].xpath(".//text()")).strip()
                if text:
                    item['product_count'] = int(text.replace(',', ''))
        except:
            pass
        return item

    def parse_td_15(self, td):
        """
        解析第16列: 市场分析 (Price, Reviews, Rating)
        包含平均价格、评论数、评分
        """
        # Price, Reviews, Rating
        # <div><a ...>$ 56.00</a></div>
        # <div><a ...>195</a><a ...>(4.8)</a></div>
        item = {'avg_price': 0.0, 'review_count': 0, 'rating': 0.0}
        try:
            price_node = td.xpath(".//a[@pop-type='price']")
            if price_node:
                text = "".join(price_node[0].xpath(".//text()")).strip()
                item['avg_price'] = float(text.replace('$', '').strip())
            
            reviews_node = td.xpath(".//a[@pop-type='reviews']")
            if reviews_node:
                text = reviews_node[0].text.strip()
                item['review_count'] = int(text.replace(',', ''))
                
            rating_node = td.xpath(".//a[@pop-type='rating']")
            if rating_node:
                text = rating_node[0].text.strip()
                # (4.8) -> 4.8
                item['rating'] = float(text.replace('(', '').replace(')', '').strip())
        except:
            pass
        return item

    def parse_td_16(self, td):
        """
        解析第17列: 操作列 (Actions)
        包含历史趋势、下载、Google Trends 等图标
        """
        # Actions column
        return {}

    def parse_subtitle_row(self, row):
        """
        解析副标题行 (通常是每个关键词数据的第二行)
        包含: 所属类目, 市场周期, SPR, 标题密度
        """
        item = {}
        try:
            tds = row.xpath("./td")
            if not tds: return {}
            td = tds[0]
            
            # Main container
            container = td.xpath(".//div[contains(@class, 'd-flex align-items-center')]")
            if not container: return {}
            
            # Iterate over direct children divs
            all_divs = container[0].xpath("./div")
            for div in all_divs:
                text = "".join(div.xpath(".//text()")).strip()
                
                if "所属类目" in text:
                    cats = []
                    sub_divs = div.xpath(".//div[contains(@class, 'text-nowrap')]")
                    for sub in sub_divs:
                        full_text = "".join(sub.xpath(".//text()")).strip()
                        # 尝试分离名称和百分比 (例如: "Toys & Games (12.5%)" 或 "Plush Toys")
                        # 匹配末尾的括号内容
                        m = re.search(r"^(.*?)\s*(\(([\d\.\-%]+)\))?$", full_text)
                        if m:
                            cat_name = m.group(1).strip()
                            cat_rate = m.group(3) if m.group(3) else None
                            cats.append({
                                "name": cat_name,
                                "rate": cat_rate
                            })
                        else:
                            # Fallback
                            cats.append({"name": full_text, "rate": None})
                            
                    item['categories'] = cats
                    
                elif "市场周期" in text:
                    val = text.replace("市场周期:", "").strip()
                    item['market_cycle'] = val
                    
                elif "SPR" in text:
                    spans = div.xpath(".//span[contains(@class, 'text-muted')]")
                    if spans:
                        item['spr'] = spans[0].text.strip()
                        
                elif "标题密度" in text:
                    spans = div.xpath(".//span[contains(@class, 'text-muted')]")
                    if spans:
                        item['title_density'] = spans[0].text.strip()
        except Exception as e:
            print(f"Error parsing subtitle row: {e}")
        return item

def parse_keyword_data(html_content):
    """
    解析HTML内容提取关键词数据
    :param html_content: HTML字符串
    :return: 提取的数据列表
    """
    # 使用 lxml 解析 HTML
    parser = etree.HTMLParser()
    tree = etree.fromstring(html_content, parser)
    
    rows = []
    # 优先使用指定 ID 查找 (针对完整页面)
    # 用户提示 XPath: //*[@id="table-condition-search"]
    container = tree.xpath('//*[@id="table-condition-search"]')
    
    if container:
        target = container[0]
        # 用户指出该节点下只有 thead 和 tbody，且结构一致
        # 直接查找 tbody 下的 tr
        rows = target.xpath("./tbody/tr")
            
    # 如果上述方式没找到行 (例如测试片段中没有该 ID)，使用通用回退策略
    if not rows:
        return []
    
    results = []
    
    parser_obj = KeywordRowParser()
    
    # 每3行一组数据
    # row[i]: 主要数据
    # row[i+1]: 副标题数据
    # row[i+2]: 分隔符 (忽略)
    i = 0
    while i + 2 < len(rows):
        try:
            row_main = rows[i]
            row_sub = rows[i+1]
            # row_sep = rows[i+2] # 忽略
            
            # 解析主数据
            item = parser_obj.parse(row_main)
            
            # 解析并合并副标题数据
            item.update(parser_obj.parse_subtitle_row(row_sub))
            
            results.append(item)
        except Exception as e:
            print(f"Error parsing group at index {i}: {e}")
            
        # 移动到下一组
        i += 3

    return results

def save_to_mongo(data):
    """
    将解析的数据保存到 MongoDB
    """
    try:
        db = get_database()
        collection = db['keywords']
        count = 0
        for item in data:
            if 'name' in item:
                # 使用 name 作为唯一键进行更新或插入
                collection.update_one(
                    {'name': item['name']},
                    {'$set': item},
                    upsert=True
                )
                count += 1
                
        print(f"Successfully saved/updated {count} items to MongoDB (keywords).")
    except Exception as e:
        print(f"Error saving to MongoDB: {e}")

if __name__ == "__main__":
    with open('ara_202512/keywords_page_1.html', 'r', encoding='utf-8') as f:
        html_content = f.read()

    print("Loaded HTML content from keywords_page.html")

    data = parse_keyword_data(html_content)
    # print(json.dumps(data, indent=2, ensure_ascii=False))
    print(f"\nSuccessfully parsed {len(data)} items.")
    
    # 保存到 MongoDB
    # save_to_mongo(data)
    
    # 测试代码
    # current_dir = os.path.dirname(os.path.abspath(__file__))
    # ref_file_path = os.path.join(current_dir, '..', '..', 'ui', '参考数据.md')
    #
    # if os.path.exists(ref_file_path):
    #     with open(ref_file_path, 'r', encoding='utf-8') as f:
    #         content = f.read()
    #
    #     # 提取 HTML 表格部分
    #     start_idx = content.find('<tr')
    #     end_idx = content.rfind('</tr>')
    #
    #     if start_idx != -1 and end_idx != -1:
    #         html_content = f"<table>{content[start_idx:end_idx+5]}</table>"
    #         data = parse_keyword_data(html_content)
    #         print(json.dumps(data, indent=2, ensure_ascii=False))
    #         print(f"\nSuccessfully parsed {len(data)} items.")
    #     else:
    #         print("Could not find HTML table rows in the file.")
    # else:
    #     print(f"File not found: {ref_file_path}")
