import os
from time import time, sleep
from datetime import date, datetime
import shutil


def main():
    interval = 60 * 60  # We run the procedure every hour

    folder_combinations = [('/data/real_time_data/bne_traffic', '/bne_traffic'),
                           ('/data/jc_decaux', '/jcdecaux_cycle'),
                           ('/data/citycycle', '/brisbane_citycycle'),
                           ('/data/gtfs_bne', '/translink_gtfs')]

    def get_output_name_and_future():
        t = time()
        d = date.fromtimestamp(t)  # for the current day
        d2 = date.fromtimestamp(t + 3600)
        return ['{}-{}-{}'.format(d.year, d.month, d.day), '{}-{}-{}'.format(d2.year, d2.month, d2.day)]

    def formated_time():
        return datetime.fromtimestamp(time()).strftime("%A, %d. %B %Y %I:%M%p")

    while True:
        print('Searching for files to move on {}'.format(formated_time()))
        counter = 0
        failed = 0
        outputs = get_output_name_and_future()
        for orig, dest in folder_combinations:
            files = os.listdir(orig)
            moveable = []
            for f in files:
                if '.sqlite' in f:
                    added = True
                    for q in outputs:
                        if q in f:
                            added = False
                    if added:
                        moveable.append(f)

            for f in moveable:
                try:
                    target = os.path.join(dest, f)
                    src = os.path.join(orig, f)
                    shutil.move(src, target)
                    print('Moved {} from {} to {}'.format(f, orig, dest))
                    counter += 1
                except:
                    print('COULD NOT Move {} from {} to {}'.format(f, orig, dest))
                    failed += 1

        print('     Found {} files to move. {} succeeded & {} failed'.format(counter + failed, counter, failed))
        print('Sleeping until {}'.format(formated_time()))
        sleep(interval)


if __name__ == '__main__':
    main()
