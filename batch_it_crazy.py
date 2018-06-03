# In[ ]:
#  coding: utf-8

###### Searching and Downloading Google Images to the local disk ######

# Import Libraries
import time  # Importing the time library to check the time of code execution
import sys  # Importing the System Library
import os
import argparse
import ssl
import pathlib
import traceback
import requests
from bs4 import BeautifulSoup
from threading import Thread
import threading
from queue import Queue
import datetime

version = (3, 0)
cur_version = sys.version_info
if cur_version >= version:  # If the Current Version of Python is 3.0 or above
    # urllib library for Extracting web pages
    from urllib.request import Request, urlopen
    from urllib.request import URLError, HTTPError
    from urllib.parse import quote
else:  # If the Current Version of Python is 2.x
    # urllib library for Extracting web pages
    from urllib2 import Request, urlopen
    from urllib2 import URLError, HTTPError
    from urllib import quote
    import urllib2

link_queue = Queue()
data_write_queue = Queue(maxsize=1000)

url_params = {'color':
                  {'gray':'ic:gray',
                   'rgb':'ic:color'},
              'usage_rights':
                  {'labled-for-reuse-with-modifications':'sur:fmc','labled-for-reuse':'sur:fc',
                   'labled-for-noncommercial-reuse-with-modification':'sur:fm',
                   'labled-for-nocommercial-reuse':'sur:f'},
              'size':
                  {'large':'isz:l',
                   'medium':'isz:m',
                   'icon':'isz:i'},
              'type':
                  {'face':'itp:face',
                   'photo':'itp:photo',
                   'clip-art':'itp:clip-art',
                   'line-drawing':'itp:lineart',
                   'animated':'itp:animated'},
              'time':{'past-24-hours':'qdr:d',
                      'past-7-days':'qdr:w'}
              }


# Downloading entire Web Document (Raw Page Content)
def download_page(url):
    version = (3, 0)
    cur_version = sys.version_info
    if cur_version >= version:  # If the Current Version of Python is 3.0 or above
        import urllib.request  # urllib library for Extracting web pages
        try:
            headers = {}
            headers[
                'User-Agent'] = "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.36"
            req = urllib.request.Request(url, headers=headers)
            resp = urllib.request.urlopen(req)
            respData = str(resp.read())
            return respData
        except Exception as e:
            print(str(e))
    else:  # If the Current Version of Python is 2.x
        try:
            headers = {}
            headers[
                'User-Agent'] = "Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.27 Safari/537.17"
            req = urllib2.Request(url, headers=headers)
            try:
                response = urllib2.urlopen(req)
            except URLError:  # Handling SSL certificate failed
                context = ssl._create_unverified_context()
                response = urlopen(req, context=context)
            page = response.read()
            return page
        except:
            return "Page Not found"


def _download_page(url):
    req = requests.get(url)
    html = req.content
    return html.encode('utf-8')


def _fetch_image_links(html):
    soup = BeautifulSoup(html, 'html.parser')
    links = soup.find_all('a', href=True)
    links = [a['href'] for a in links]
    return links


# Finding 'Next Image' from the given raw page
def _images_get_next_item(s):
    start_line = s.find('rg_di')
    if start_line == -1:  # If no links are found then give an error!
        end_quote = 0
        link = "no_links"
        return link, end_quote
    else:
        start_line = s.find('"class="rg_meta"')
        start_content = s.find('"ou"', start_line + 1)
        end_content = s.find(',"ow"', start_content + 1)
        content_raw = str(s[start_content + 6:end_content - 1])
        return content_raw, end_content


# Getting all links with the help of '_images_get_next_image'
def _images_get_all_links(page):
    items = []
    while True:
        item, end_content = _images_get_next_item(page)
        if item == "no_links":
            break
        else:
            items.append(item)  # Append all the links in the list named 'Links'
            time.sleep(0.1)  # Timer could be used to slow down the request for image downloads
            page = page[end_content:]
    return items


#Building URL parameters
def build_url_parameters(**kwargs):
    built_url = "&tbs="
    params = []

    for key, value in kwargs.items():
        param = url_params[key][value]
        params.append(param)

    return '%tbs=%s' + ','.join(params)


# build the URL for Google
def _build_url(search_term, engine='google', params=''):
    url = 'https://www.google.com/search?q={0}&espv=2&biw=1366&bih=667&site=webhp&source=lnms&tbm=isch{1}&sa=X&ei=XosDVaCXD8TasATItgE&ved=0CAcQ_AUoAg'
    # add the keyword to the URL
    url = url.format(search_term, params)

    return url


def type_check(v, o):
    if not isinstance(v, o):
        # get the stack trace so we can get the name of the variable
        stack = traceback.extract_stack()
        # get the name of this function (just in case it gets changed in the future)
        (_, _, function_name, _) = stack[-1]
        # get the text that was typed for this call
        (_, _, _, text) = stack[-2]
        # parse the variable names from the text
        # - now it's "type_check(my_var, str)"
        # - but can be "x = [type_check(y, int) for y in my_list]"
        # - so we need to make the string parsing robust to account for the variations
        vars = text.split('%s(' % function_name)[1]
        vars = [var.strip() for var in vars.split(')')[0].split(',')]
        var_name = vars[0]
        type_name = vars[1]

        # now we get to raise the error... finally!
        raise ValueError('Invalid type: Variable "{}" must be of type {}.'.format(var_name, type_name))


def _build_urls(keywords, addendums, **kwargs):

    urls = []
    params = build_url_parameters(**kwargs)

    for category, terms in keywords.items():
        # build keywords + addendums and create all their URLs
        for term in terms:
            search_terms = [('"%s": %s' % (addendum, term)).replace(':', '%3A').replace(' ', '+') for addendum in addendums]
            _urls = [_build_url(search_term, params=params) for search_term in search_terms]
            _urls = [(category, url) for url in _urls]
            urls += _urls

    return urls


def _get_all_links(url_batch, verbose=False):
    links = {}
    t_name = threading.current_thread().name

    for category, url in url_batch:
        # fetch html
        html = download_page(url)
        new_links = _images_get_all_links(html)
        num_links_added = 0

        # this makes sure that all the links are unique
        for link in new_links:
            if link not in links:
                num_links_added += 1
                links = {value for value in list(links) + [link]}

        link_queue.put((category, links))
        link_queue.task_done()
        if verbose:
            print('%s [%s]: %d links added' % (t_name, category, num_links_added))


def _get_unique_links(kw_links, cross_filter, verbose=False):
    t_name = threading.current_thread().name
    category_links = {}
    filtered_category_links = []
    num_links_removed = 0

    for category, links in kw_links:
        if category not in category_links:
            category_links[category] = {}

        for link in links:
            if link not in category_links[category]:
                category_links[category][link] = 1
            else:
                category_links[category][link] += 1

    # perform category cross-filtering
    if cross_filter == 'None':
        for category in category_links.keys():
            for link in category_links[category].keys():
                filtered_category_links.append((category, link))

    elif cross_filter == 'Count':
        categories = list(category_links.keys())

        for i, category in enumerate(categories[:-1]):
            other_categories = categories[i + 1:]

            # iterate over each link in this category
            for curr_link, curr_link_count in category_links[category].items():

                keep_link = True

                # iterate over each other category and look for this link
                for other_category in other_categories:
                    if curr_link in category_links[other_category]:
                        other_link_count = category_links[other_category][curr_link]

                        # LOSE: dont keep this link
                        if curr_link_count < other_link_count:
                            keep_link = False
                            num_links_removed += 1
                            break

                        # TIE: dont keep any links
                        elif curr_link_count == other_link_count:
                            del category_links[other_category][curr_link]
                            keep_link = False
                            num_links_removed += 2
                            break

                        # WIN: remove the other link
                        else:
                            num_links_removed += 1
                            del category_links[other_category][curr_link]

                if keep_link:
                    filtered_category_links.append((category, curr_link))

    elif cross_filter == 'Strict':
        categories = list(category_links.keys())

        for i, category in enumerate(categories[:-1]):
            other_categories = categories[i + 1:]

            # iterate over each link in this category
            for link in category_links[category].keys():

                keep_link = True

                # iterate over each other category and look for this link
                for other_category in other_categories:
                    if link in category_links[other_category]:
                        keep_link = False
                        del category_links[other_category][link]
                        num_links_removed += 2

                if keep_link:
                    filtered_category_links.append((category, link))

    else:
        raise ValueError('\'{}\': Invalid argument for cross_filter parameter.'.format(cross_filter))

    if verbose:
        print('%s cross_filter=%s: %d links removed' % (t_name, cross_filter, num_links_removed))

    return filtered_category_links


def _write_data_thread(verbose=False):
    t_name = threading.current_thread().name
    while True:
        directory, data = data_write_queue.get()
        file_id = len(os.listdir(directory))
        filename = '%d.jpg' % file_id
        filename = os.path.join(directory, filename)
        output_file = open(filename, 'wb')
        output_file.write(data)
        output_file.close()
        data_write_queue.task_done()
        if verbose:
            print('%s >>> [%s] file %d written!' % (t_name, os.path.basename(directory), file_id))


def _fetch_images(links, out_dir, log_out_dir, verbose=False):
    t_name = threading.current_thread().name
    log_filename = time.strftime('log-{} %d-%m-%Y %H-%M.txt', time.localtime()).format(t_name)
    log_filename = os.path.join(log_out_dir, log_filename)
    logfile = open(log_filename, 'w')
    logfile.write('Image Count,Status,Url,FileName,Message\n')

    supported_formats = ('.jpg', '.jpeg', '.png', '.svg')

    for i, (category, link) in enumerate(links):

        image_name = ''

        try:
            req = Request(link, headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux i686) AppleWebKit/537.17 (KHTML, like Gecko) Chrome/24.0.1312.27 Safari/537.17"})
            response = urlopen(req, None, 15)
            image_name = str(link[(link.rfind('/')) + 1:])

            if '?' in image_name:
                image_name = image_name[:image_name.find('?')]

            ext = os.path.splitext(image_name)[-1]
            if ext in supported_formats:
                # get data stream
                data = response.read()
                category_dir = os.path.join(out_dir, category)
                data_write_queue.put((category_dir, data))
                #write_queue.task_done()
                response.close()

                log_message = '%d,downloaded,%s,%s,\n' % (i+1, link, image_name)
                if verbose:
                    print('%s (%d) downloaded: %s' % (t_name, i+1, image_name))
            else:
                log_message = "%d,Exception,,,InvalidExtension: '%s' is not a supported image format. Supported formats %s\n" \
                              % (i+1, ext, str(supported_formats))
                if verbose:
                    print('%s (%d) [Exception] UnsupportedExtension: %s' % (t_name, i+1, ext))

        except Exception as e:
            error_name = str(type(e)).split('\'')[1]
            error_message = str(e)
            image_name = 'N/A' if image_name == '' else image_name

            log_message = "%d,Exception,%s,%s,%s: %s\n" % (i, link, image_name, error_name, error_message)
            if verbose:
                print('%s (%d) [Exception] %s: %s' % (t_name, i+1, error_name, error_message))
                print('\tLink: %s\n\tImg Name: %s' % (link, image_name))

        logfile.write(log_message)

    logfile.close()


def _create_batches(data, num_batches=20):
    type_check(data, list)
    num_threads = min(num_batches, len(data))
    batch_size = int(len(data) / num_threads)
    batches = []

    while len(data) > 0:
        batch = data[:batch_size]
        data = data[batch_size:]
        batches.append(batch)

    return batches


def go_go_batch_it(keywords, addendums=None, out_dir='output', log_out_dir='logs',
                   verbose=False, num_threads=20, cross_filter='Count', **kwargs):

    if verbose:
        print('initializing Batch-It')
        print('validating parameters...')

    # init_time
    init_t = time.time()
    output_dirs = {}

    # check that the out_dir is a path
    if not os.path.isdir(out_dir):
        raise ValueError('Invalid Argument: Variable "out_dir" must be a directory.')
    for key in keywords.keys():
        d = os.path.join(out_dir, key)
        if not os.path.isdir(d):
            os.mkdir(d)
        output_dirs[d] = len(os.listdir(d))

    if not os.path.isdir(log_out_dir):
        os.mkdir(log_out_dir)

    # check that the keyword parameter is of type list
    type_check(keywords, dict)
    for key, value in keywords.items():
        type_check(key, str)
        type_check(value, list)
        for elem in value:
            type_check(elem, str)

    # verify cross_filter
    if cross_filter not in ('None', 'Count', 'Strict'):
        err_message = '\'{}\': Invalid argument for cross_filter. Must be one of {}'.format(cross_filter, ('None', 'Count', 'Strict'))
        raise ValueError(err_message)

    # repeate the checking process for addendumns if it's not None
    if addendums is not None:
        type_check(addendums, list)
        for addendum_elems in addendums:
            type_check(addendum_elems, str)
    else:
        # this makes it easier for later
        addendums = []

    if verbose:
        print('%d categories found' % len(keywords.keys()))
        print('building urls...')

    urls = _build_urls(keywords, addendums)
    batches = _create_batches(urls, num_batches=num_threads)

    # spin up threads to get ALL UNIQUE LINKS (this is unique per category)
    if verbose:
        print('%d urls created' % len(urls))
        print('spinning up LinkFetcher threads...')
    threads = [Thread(target=_get_all_links, name='t-[LinkFetcher %d]' % (i+1), args=(batch, verbose))
               for i, batch in enumerate(batches)]
    for t in threads:
        if verbose:
            print('starting %s' % t.name)
        t.start()
    for t in threads:
        t.join()

    links = []
    while not link_queue.empty():
        links.append(link_queue.get())
    # _get_unique_links & cross_filter will compare links between categories
    # cross_filter='None' will not perform any cross-category filtering
    # cross_filter='Count' will filter out links in categories if their link count is lower than anothers
    # cross_filer='Strict' will through out any links which are present in 2 or more categories
    links = _get_unique_links(links, cross_filter=cross_filter, verbose=verbose)

    if verbose:
        print('%d links found' % len(links))
        print('batching links & spinning up download threads...')

    # Now batch up the links
    batches = _create_batches(links, num_threads)

    # create write thread
    write_t = Thread(target=_write_data_thread, name='t-[write]', args=(verbose,))
    write_t.start()

    # create threads to fetch images
    threads = [Thread(target=_fetch_images, name='t-[Downloader %d]' % (i+1), args=(batch, out_dir, log_out_dir,verbose))
               for i, batch in enumerate(batches)]
    for t in threads:
        if verbose:
            print('starting %s' % t.name)
        t.start()
    for t in threads:
        t.join()

    data_write_queue.join()
    link_queue.join()

    final_t = time.time()

    if verbose:
        total_t = int((final_t - init_t))
        total_files_written = 0
        for d, original_count in output_dirs.items():
            new_count = len(os.listdir(d))
            new_item_count = new_count - original_count
            total_files_written += new_item_count
            output_dirs[d] = new_item_count

        print('done!')
        print('----------------------------')
        print('Summary:')
        print('Total Num. Files Saved: %d' % total_files_written)
        print('Total Time            : %d' % total_t)
        print('Files / Second        : %.2ff/s' % (float(total_files_written) / total_t))
        print('Files by Category:')
        for d, count in output_dirs.items():
            print('\t%s: %d' % (os.path.basename(d), count))


# start of main program
if __name__ == '__main__':
    import argparse
    import json

    parser = argparse.ArgumentParser()
    parser.add_argument('--search_args_path', type=str, required=True,
                        help='Search Arguments JASON: \n'
                             '\t{\'keywords\': {category_label: [search terms]},'
                             '\t  \'addendums\': [additional attributes]}')
    parser.add_argument('--out_dir', type=str, required=False,
                        help='Directory for outputs',
                        default='output')
    parser.add_argument('--log_out_dir', type=str, required=False,
                        help='Output directory for log files',
                        default='logs')
    parser.add_argument('--verbose', type=bool, required=False,
                        help='Display verbose info to std.out',
                        default=False)
    parser.add_argument('--num_threads', type=int, required=False,
                        help='Max number of threads to run',
                        default=20)
    parser.add_argument('--cross_filter', type=str, required=False,
                        help='Type of cross-category filtering. This helps reduce number of duplicate images copied '
                             'to different categories. Types=(None, Count, Strict)',
                        default='Count')
    parser.add_argument('--color', type=str, required=False, default='rgb')
    parser.add_argument('--type', type=str, required=False, default=None)

    args = parser.parse_args()

    search_data = json.load(open(args.search_args_path))
    keywords = search_data['keywords']
    addendums = search_data['addendums']
    imcolor = args.color
    imtype = args.type

    go_go_batch_it(keywords, addendums,
                   out_dir=args.out_dir,
                   log_out_dir=args.log_out_dir,
                   verbose=args.verbose,
                   num_threads=args.num_threads,
                   cross_filter=args.cross_filter,
                   color=imcolor,
                   type=imtype)

