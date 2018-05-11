import urllib.request
from bs4 import BeautifulSoup
import time
import datetime
import requests
from multiprocessing import Process
import sqlite3
import sys

conn = sqlite3.connect('PornHubDB.db')
db = conn.cursor()


class AllPagesCrawler:
    def __init__(self, url):
        self.starter = 0
        self.ender = 10000000
        self.url = url
        self.fullpage = {}
        if sys.argv[1] == 'all':
            self.starter = 0
        else:
            self.starter = sys.argv[1]
        if sys.argv[2] == 'all':
            self.ender = 100000000
        else:
            self.ender = sys.argv[2]

    def getting_full_page(self):
        while True:
            if self.starter - 1 != self.ender:
                response = requests.get(str(self.url) + '&page=' + str(self.starter)).text
                soup = BeautifulSoup(response, 'lxml')
                try:
                    soup.find('div', class_='noVideosNotice').find('em').contents == 'No videos found.'
                    print('All Pages Already indexed')
                    return
                except AttributeError:
                    # Finding all video blocks
                    table = soup.find('ul', class_='nf-videos videos search-video-thumbs')

                for li in table.find_all('li'):
                    length = ''.join(li.find('var', class_='duration').contents).replace(':', '.')
                    if ''.join(li.find('var', class_='hd-thumbnail').contents) == 'HD':
                        is_hd = True
                    else:
                        is_hd = False
                    name  = ''.join(li.find('span', class_ = 'title').find('a').contents)
                    self.fullpage = ({'ph_id': li.get('_vkey'), 'length_in_sec': int(
                        (int(float(length)) * 60) + ((float(length) - int(float(length))) * 100)), 'is_hd': is_hd, 'name': name})
                    # ---------------------------------------------DATA BASE------------------------------------------------


                    db.execute('INSERT INTO index (ph_id, length_in_sec, is_hd, name) VALUES ( {fullpage[ph_id]}, {fullpage[length_in_sec]}, {fullpage[is_hd]}, {fullpage[name]})'.format(fullpage=self.fullpage))
                self.fullpage.clear()
                self.starter += 1
            else:
                print('All Pages Already indexed')
                return

class OneVideoCrawler:
    def __init__(self):
        self.fullvideo = {}

    def getting_full_video(self):

        videoresponse = requests.get('https://www.pornhub.com/view_video.php?viewkey=' + 'vkey').text
        videosoup = BeautifulSoup(videoresponse, 'lxml')

        try:
            if videosoup.find('div',
                              class_='text pending').contents == 'Video has been flagged by the community and is currently disabled pending review.':
                return
        except:
            # ---------------------------------------------CATEGORIES-------------------------------------------------------

            categories = []
            try:
                for g in videosoup.find('div', class_='categoriesWrapper').find_all('a')[:-1]:
                    categories.append(''.join(g.contents))
            except KeyError:
                pass

            self.fullvideo['categories'] = categories

            views = (''.join(videosoup.find('div', class_='views').find('span', class_='count').contents))

            self.fullvideo['views'] = views
            # ----------------------------------------------FROM------------------------------------------------------------

            try:
                fromer = ''.join(
                    videosoup.find('div', class_='video-info-row').find('div', class_='usernameWrap clearfix').find(
                        'a').contents)

            except AttributeError:
                fromer = 'Null'

            self.fullvideo['From'] = fromer
            # ----------------------------------------------PORNSTARS-------------------------------------------------------

            pornstars = []
            try:
                for pornstar in videosoup.find('div', class_='pornstarsWrapper').find_all('a')[:-1]:
                    pornstars.append(pornstar.attrs['data-mxptext'])

                self.fullvideo['Pornstars'] = pornstars
            except KeyError:
                pass
            # ----------------------------------------------TAGS--------------------------------------------------------

            tags = []
            try:
                for tag in (videosoup.find('div', class_='tagsWrapper').find_all('a'))[:-1]:
                    tags.append(''.join(tag.contents))
            except KeyError:
                pass

            self.fullvideo['tags'] = tags
            # ----------------------------------------------PRODUCTION--------------------------------------------------

            try:
                for product in (videosoup.find('div', class_='productionWrapper').find_all('a')[:-1]):
                    if ''.join(product.contents) == 'professional':
                        is_professional = True
                    else:
                        is_professional = False
            except KeyError:
                is_professional = 'Null'

            self.fullvideo['is_professional'] = is_professional
            # ----------------------------------------------LIKE--------------------------------------------------------

            likes = (int(''.join(videosoup.find('span', class_='votesUp').contents)))
            self.fullvideo['likes'] = likes
            # ----------------------------------------------DISLIKE-----------------------------------------------------

            dislikes = (int(''.join(videosoup.find('span', class_='votesDown').contents)))
            self.fullvideo['dislikes'] = dislikes
            # --------------------------------------------------Times---------------------------------------------------

            try:

                if str(''.join(videosoup.find_all('span', class_='white')[1].contents)).replace(' ', '') == 'Today':
                    featured = 'sec'
                    numbersifeautured = 1
                elif str(''.join(videosoup.find_all('span', class_='white')[1].contents)).replace(' ',
                                                                                                  '') == 'Yesterday':
                    featured = 'day'
                    numbersifeautured = 1
                else:
                    featured = 'featured: ' + str(''.join(videosoup.find_all('span', class_='white')[1].contents))
                    numbersifeautured = int(
                        str(''.join(videosoup.find_all('span', class_='white')[1].contents)[:2]).replace(' ', ''))
                datetimefeautered = datetime.datetime.fromtimestamp(
                    time.time() - self.ifs(featured, numbersifeautured)).replace(
                    microsecond=0)
            except IndexError:
                pass

            if str(''.join(videosoup.find_all('span', class_='white')[0].contents)).replace(' ', '') == 'Today':
                added_on = 'sec'
                numbersiadded = 1
            elif str(''.join(videosoup.find_all('span', class_='white')[0].contents)).replace(' ', '') == 'Yesterday':
                added_on = 'day'
                numbersiadded = 1
            else:
                added_on = 'added_on: ' + str(''.join(videosoup.find_all('span', class_='white')[0].contents))
                numbersiadded = int(
                    str(''.join(videosoup.find_all('span', class_='white')[0].contents)[:2]).replace(' ', ''))
            datetimeaddedto = datetime.datetime.fromtimestamp(time.time() - self.ifs(added_on, numbersiadded)).replace(
                microsecond=0)

            try:

                self.fullvideo['Featured_date'] = str(datetimefeautered)
                self.fullvideo['AddedToPh'] = str(datetimeaddedto)
            except UnboundLocalError:

                self.fullvideo['Featured_date'] = 'Null'
                self.fullvideo['AddedToPh'] = str(datetimeaddedto)

    def ifs(self, ins, numbertypes):
        if 'year' in ins:
            timestmp = numbertypes * 3.154e+7

        elif 'month' in ins:
            timestmp = numbertypes * 2.628e+6

        elif 'week' in ins:
            timestmp = numbertypes * 604800
        elif 'day' in ins:
            timestmp = numbertypes * 86400

        elif 'hour' in ins:
            timestmp = numbertypes * 3600

        elif 'minute' in ins:
            timestmp = numbertypes * 60

        elif 'sec' in ins:
            timestmp = numbertypes

        return timestmp

    def selecting(self):
        pass


a = AllPagesCrawler()
a.getting_full_page()
