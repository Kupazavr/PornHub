import urllib.request
from bs4 import BeautifulSoup
import time
import datetime
import requests
from multiprocessing import Process
import sqlite3
import sys

conn = sqlite3.connect('D:\sqlite\PornHub.db')
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
                        is_hd = 1
                    else:
                        is_hd = 0
                    name = ''.join(li.find('span', class_='title').find('a').contents)
                    self.notfullpage = ({'ph_id': li.get('_vkey'), 'length_in_sec': int(
                        (int(float(length)) * 60) + ((float(length) - int(float(length))) * 100)), 'is_hd': is_hd,
                                         'name': name})
                    # ---------------------------------------------DATA BASE------------------------------------------------

                    if db.execute('SELECT ph_id FROM indexing WHERE ph_id = {fullpage[ph_id]};'.format(
                            fullpage=self.notfullpage)).fetchone() == None:
                        db.execute(
                            'INSERT INTO index (ph_id, length_in_sec, is_hd, name) VALUES ( {fullpage[ph_id]}, {fullpage[length_in_sec]}, {fullpage[is_hd]}, {fullpage[name]})'.format(
                                fullpage=self.notfullpage))
                    else:
                        continue

                self.fullpage.clear()
                self.starter += 1
            else:
                print('All Pages Already indexed')
                return


class OneVideoCrawler:
    def __init__(self):
        self.fullvideo = {}

    def getting_full_video(self, vkey):
        self.fullvideo['vkey'] = vkey
        videoresponse = requests.get('https://www.pornhub.com/view_video.php?viewkey=' + vkey).text
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

            self.fullvideo['categories'] = ','.join(categories)
            # ------------------------------------------Views------------------------------------------------------------
            views = (''.join(videosoup.find('div', class_='views').find('span', class_='count').contents))

            self.fullvideo['views'] = views
            # ----------------------------------------------FROM------------------------------------------------------------

            try:
                fromer = ''.join(
                    videosoup.find('div', class_='video-info-row').find('div', class_='usernameWrap clearfix').find(
                        'a').contents)

            except AttributeError:
                fromer = 'NULL'

            self.fullvideo['From'] = fromer
            # ----------------------------------------------PORNSTARS-------------------------------------------------------

            pornstars = []
            try:
                for pornstar in videosoup.find('div', class_='pornstarsWrapper').find_all('a')[:-1]:
                    pornstars.append(pornstar.attrs['data-mxptext'])

                self.fullvideo['Pornstars'] = ','.join(pornstars)
            except KeyError:
                pass
            # ----------------------------------------------TAGS--------------------------------------------------------

            tags = []
            try:
                for tag in (videosoup.find('div', class_='tagsWrapper').find_all('a'))[:-1]:
                    tags.append(''.join(tag.contents))
            except KeyError:
                pass

            self.fullvideo['tags'] = ','.join(tags)
            # ----------------------------------------------PRODUCTION--------------------------------------------------

            try:
                for product in (videosoup.find('div', class_='productionWrapper').find_all('a')[:-1]):
                    if ''.join(product.contents) == 'professional':
                        is_professional = 1
                    else:
                        is_professional = 0
            except KeyError:
                is_professional = 'NULL'

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

                self.fullvideo['featured_date'] = str(datetimefeautered)
                self.fullvideo['AddedToPh'] = str(datetimeaddedto)
            except UnboundLocalError:

                self.fullvideo['featured_date'] = 'NULL'
                self.fullvideo['AddedToPh'] = str(datetimeaddedto)

            self.fullvideo['datetime'] = datetime.datetime.now().replace(microsecond=0)
            # --------------------------------------------Database------------------------------------------------------------------
            # РИСКОВЫЙ БЛОК

            # tags
            db.execute('INSERT INTO tags (name) VALUES ({fullvideo[tags]})'.format(fullvideo=self.fullvideo))
            # videotags
            vtg = db.execute(
                'SELECT id FROM tags WHERE name = "{fullvideo[tags]}"'.format(fullvideo=self.fullvideo)).fetchall()[-1][
                0]
            db.execute('INSERT INTO videotags (tags, ph_id, added) VALUES ("' + str(
                vtg) + '", "{fullvideo[vkey]}", {fullvideo[datetime]})'.format(fullvideo=self.fullvideo))

            # pornstars
            db.execute('INSERT INTO pornstars (name) VALUES ({fullvideo[pornstars]})'.format(fullvideo=self.fullvideo))
            # videopornstars
            vps = db.execute('SELECT id FROM pornstars WHERE name = "{fullvideo[pornstars]}"'.format(
                fullvideo=self.fullvideo)).fetchall()[-1][0]
            db.execute('INSERT INTO videopornstars (pornstars, ph_id, added) VALUES ("' + str(
                vps) + '", "{fullvideo[vkey]}", {fullvideo[datetime]})'.format(fullvideo=self.fullvideo))

            # categories
            db.execute(
                'INSERT INTO categories (name) VALUES ({fullvideo[categories]})'.format(fullvideo=self.fullvideo))
            # videocategories
            vct = db.execute('SELECT id FROM categories WHERE name = "{fullvideo[categories]}"'.format(
                fullvideo=self.fullvideo)).fetchall()[-1][0]
            db.execute('INSERT INTO videocategories (categories, ph_id, added) VALUES ("' + str(
                vct) + '", "{fullvideo[vkey]}", {fullvideo[datetime]})'.format(fullvideo=self.fullvideo))

            # viewshistory
            db.execute(
                'INSERT INTO videocategories (ph_id, views, like, dislike, datetime) VALUES ("{fullvideo[vkey]}", "{fullvideo[views]}", {fullvideo[likes]}, {fullvideo[dislikes]}, {fullvideo[datetime]})'.format(
                    fullvideo=self.fullvideo))

            # from
            if db.execute('SELECT producer FROM indexing WHERE ph_id = "{fullvideo[vkey]}"'.format(
                    fullvideo=self.fullvideo)) == None:
                # db.execute('INSERT INTO from (ph_id, name) VALUES ("{fullvideo[vkey]}", {fullvideo[from]})'.format(fullvideo=self.fullvideo))
                db.execute('UPDATE from SET name = {fullvideo[from]}'.format(fullvideo=self.fullvideo))
            else:
                return

            # ----------------------------------MAINTABLE----------------------------------------------------------------
            db.execute(
                'UPDATE indexing SET producer = "{fullvideo[from]}", is_professional = {fullvideo[is_professional]}, featured_date = "{fullvideo[featured_date]}", added_date = "{fullvideo[AddedToPh]}")'.format(
                    fullvideo=self.fullvideo))

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


b = OneVideoCrawler()

# ------------------------------------------DATABASESELECT---------------------------------------------------------------
db.execute('SELECT ph_id FROM indexing')
for phid in db.execute('SELECT ph_id FROM indexing').fetchone():
    # ПОВЕСИТЬ НА ПРОЦЕССЫ
    b.getting_full_video(phid)
