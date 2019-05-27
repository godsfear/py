#!/python

from xfuncs import *
import requests
import sys
install('colorama')
import colorama
from colorama import Fore, Back, Style

def main():
    colorama.init(autoreset=True)
    print(Fore.WHITE + Back.BLACK + 'Red foreground text')
    print(Back.BLACK + Fore.GREEN + 'Red background text')
    print("Some plain text")

if __name__ == '__main__':
    main()
