import locale
import argparse


class Brands(object):
    filename = 'brands.csv'
    locale_lc_all = 'en_US.UTF-8'

    def __init__(self):
        self.brands = self.get(self.filename)

    def get(self, filename):
        with open(filename, 'r') as f:
            return f.readlines()

    def sort(self):
        locale.setlocale(locale.LC_ALL, self.locale_lc_all)
        self.brands = sorted(
            set(self.brands),
            cmp=locale.strcoll,
            key=lambda x: x.lower()
        )
        return self

    def write(self):
        with open(self.filename, 'w') as f:
            f.writelines(
                self.brands
            )

    def add(self, filename):
        for brand in self.get(filename):
            if brand not in self.brands:
                self.brands.append(brand)
        return self

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filename', help='file contains list with new brands')
    args = parser.parse_args()
    Brands().add(args.filename).sort().write()