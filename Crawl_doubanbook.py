"""
作者：Justin
日期：2018/4/23
功能：1.通过豆瓣读书标签，爬取分类
      2.通过分类的链接进入到分类页，爬取分类页图书的内容
      3.内容包括图书名，评论人数，评分级别，作者，出版社与日期
      4.把爬取的数据写入execl文件中，分类别存储
      5.运用多进程或多线程的方法，提高爬取的效率
"""
import requests
from lxml import etree
from openpyxl import Workbook
from fake_useragent import UserAgent
import time
import random
from multiprocessing import Pool, Manager, Queue
import traceback

# 下载分类标签
def get_all_linkByurl(url):
    """
    获取分链接与其分链接名字
    """
    headers = {}
    fake_user = UserAgent()
    headers['User-Agent'] = fake_user.random
    response = requests.get(url, headers=headers)
    print('正在爬取该网页...')
    #print(response)
    try:
        url_info = etree.HTML(response.text)
        #获取分类标题
        #xpath方法
        #获取分类标题
        book_class = url_info.xpath('//table[@class="tagCol"]/tbody/tr/td/a/text()')
        print('爬取完成')
        #print(book_class)
        return book_class
    except response.status_code != 200 as error:
        print('无法链接该网页，请检查')


# 通过分类标签组建url，下载图书页的内容
def get_book_detailBylink(book_class, multi_queue):
    """
    通过组建url获取10页图书内容
    """
    book_html = []
    book_html_dict = {}
    headers_get_page_num = {}
    # 由于如果全部页数提取，数据量庞大，程序运行时间过长，所以现在只提取前10页
    """
    # 提取该网页的总页数
    fake_user_get_page_num = UserAgent()
    headers_get_page_num['User-Agent'] = fake_user_get_page_num.random
    get_page_num_url = 'https://book.douban.com/tag/{}'.format(book_class)
    page_num_response = requests.get(get_page_num_url, headers=headers_get_page_num)
    page_num_html = etree.HTML(page_num_response.text)
    # 提取页数
    page_num = int(page_num_html.xpath('//*[@class="paginator"]/a[last()]/text()'))
    """
    # 提取10页
    page_num = 10
    print('Start process {}'.format(book_class))
    print('正在爬取{}内容'.format(book_class))
    for page in range(page_num):
        headers = {}
        fake_user = UserAgent()
        headers['User-Agent'] = fake_user.random
        print('正在爬取第{}页内容'.format(page+1))
        # 适当延时可以防止反爬
        time.sleep(random.randint(1, 8) * 2)
        # 组建图书详情页的url
        bookdetail_url = 'https://book.douban.com/tag/{}?start={}&type=T'.format(book_class, page*20)
        book_response = requests.get(bookdetail_url, headers=headers)
        book_info_html = etree.HTML(book_response.text)
        # 获取没有图书内容的标签
        book_info_exist = book_info_html.xpath('//div[@id="subject_list"]/ul/*')
        # 判断该页面是否已经没有图书内容
        # 没有则退出循环，节约资源
        if len(book_info_exist) > 1:
            book_html.append(book_response.text)
        elif len(book_info_exist) <= 1:
            print('本页没有图书内容')
            return None
        else:
            print('本页发生错误，请检查')
            print('错误: {}'.format(traceback.format_exc()))
            return None
    book_html_dict[book_class] = book_html
    book_dict = cleanup_data(book_class, book_html_dict)
    multi_queue.put(book_dict)
    print('存入数据队列成功')
    print('End process {}'.format(book_class))


# 提取数据
def cleanup_data(book_class, book_data_dict):
    """
    提取数据
    """
    print('进入提取数据环节')
    #print(result)
    book_dict = {}
    all_book_detail = []
    for one_page_book in book_data_dict[book_class]:
        book_html = etree.HTML(one_page_book)
        # 获取没有图书内容的标签
        # 豆瓣并不都是一页20本书，防止网站袭击
        book_total_num = book_html.xpath('//div[@id="subject_list"]/ul/*')
        # print(len(book_total_num))
        # 豆瓣有一些页并没有图书内容，需要做一个判断

        for book_num in range(1, len(book_total_num) + 1):
                print('正在爬取第{}本书'.format(book_num))
                # 获取书名
                try:
                    book_name = book_html.xpath('//ul/li[{}]/div[@class="info"]/h2/a/@title'.format(book_num))[0]
                    # print(book_name)
                except IndexError:
                    book_name = '暂无'
                # 获取书本连接
                try:
                    book_link = book_html.xpath('//ul/li[{}]/div[@class="info"]/h2/a/@href'.format(book_num))[0]
                    # print(book_link)
                except IndexError:
                    book_link = '无此书链接'
                # 获取作者名、出版社与日期
                book_info = book_html.xpath('//ul/li[{}]/div[@class="info"]/div/text()'.format(book_num))[0]
                book_info_list = book_info.strip().split('/')
                try:
                    book_writer = '/'.join(book_info_list[0:-3])
                except IndexError:
                    book_writer = '暂无'
                try:
                    book_publisher = ''.join(book_info_list[-3:-2])
                except IndexError:
                    book_publisher = '暂无'
                try:
                    book_time = ''.join(book_info_list[-2])
                except IndexError:
                    book_time = '暂无'
                # print(book_writer, book_publisher, book_time)
                # 获取评论等级
                try:
                    book_ranking_str = book_html.xpath('//ul/li[{}]/div[@class="info"]/div/span[@class="rating_nums"]/text()'
                                                       .format(book_num))
                    book_ranking = book_ranking_str[0]
                except IndexError:
                    book_ranking = '0.0'
                # 获取评论数
                try:
                    book_comment = book_html.xpath('//ul/li[{}]/div[@class="info"]/div/span[@class="pl"]/text()'
                                                   .format(book_num))
                    book_comment_num = book_comment[0].strip()[1:-1]
                except IndexError:
                    book_comment_num = '0人评论'

                # print(book_ranking, book_comment_num)
                # 将图书的信息放入列表中
                book_detail_list = [book_name, book_link, book_writer, book_publisher,
                                    book_time, book_ranking, book_comment_num]
                # print(book_detail_list)
                # 把这一分类的图书信息放入总列表中
                all_book_detail.append(book_detail_list)
        print('存入{}字典完成'.format(book_class))
        book_dict[book_class] = all_book_detail
        # print(book_dict)
    return book_dict


# 保存数据到excel
def save_data_in_excel(book_info_dict):
    """
    将数据存储到excel文件中
    """
    print('正在进行数据存储')
    # 创建一个excel文件
    save_wb = Workbook()
    save_path = r'E:\Program\Python\douban\book_date.xlsx'
    print('已经创建一个excel文件对象')
    # 文件的sheet
    save_ws = []
    # 处理字典，把类别与数据分开存储
    book_class = list(book_info_dict.keys())
    # 创建sheet，并把图书的分类类别做sheet_name
    for book_class_num in range(len(book_class)):
        save_ws.append(save_wb.create_sheet(title=book_class[book_class_num]))

    # 删除sheet，必须是Wordsheet对象才能进行删除。remove_sheet模块不能自动转成Wordsheet对象
    # 所以需要get_sheet_by_name()模块的配合
    save_wb.remove_sheet(save_wb.get_sheet_by_name('Sheet'))
    # 写入表头
    for list_header in range(len(book_class)):
        save_ws[list_header].append(['序号', '书名', '链接', '评分', '评论人数', '作者\译者', '出版社'])
        # 序号
        count = 1
        # 写入数据
        for info_list in book_info_dict[book_class[list_header]]:
            save_ws[list_header].append([count, info_list[0], info_list[1], info_list[5],
                                        info_list[6], info_list[2], info_list[3]])
            count += 1
        print('{}数据写入成功'.format(save_ws[list_header].title))
    save_wb.save(save_path)
    print('表格保存完成')


def main():
    """
    主函数
    """
    complete_flag = True
    book_info_dict = {}
    # 如果进程池之间通信格式是Queue()，则会造成资源混乱，缺少部分数据输入
    # 所以进程间通信队列的格式是Manger().Queue()
    # 若只是普通Process，进程间通信队列格式是Queue()
    # 创建队列对象
    multi_manager = Manager()
    multi_queue = multi_manager.Queue()
    url = 'https://book.douban.com/tag/?view=type&icn=index-sorttags-all'
    book_class_list = get_all_linkByurl(url)
    print('父进程开始')
    # 创建进程池
    multi_pool = Pool()
    for book_class in book_class_list[92:]:
        multi_pool.apply_async(get_book_detailBylink, (book_class, multi_queue))
    #print(multi_queue.empty())
    multi_pool.close()
    multi_pool.join()
    multi_size = multi_queue.qsize()
    # print(multi_size)
    while complete_flag:
        # 有两种情况队列为空：
        # 1、数据都转移到book_info_dict里
        # 2、没有任何数据存进队列中
        if not multi_queue.empty():
            book_info = multi_queue.get()
            book_info_dict.update(book_info)
            print(multi_queue.empty())
            if len(book_info_dict) == multi_size:
                save_data_in_excel(book_info_dict)
                complete_flag = False
        else:
            print('队列中没有数据存储')
            complete_flag = False
    print('父进程结束')


if __name__ == "__main__":
    main()