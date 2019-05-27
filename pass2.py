#!/python
from tkinter import *

def makeentry(parent, caption, width=None, **options):
    Label(parent, text=caption).pack(side=LEFT)
    entry = Entry(parent, **options)
    if width:
        entry.config(width=width)
    entry.pack(side=LEFT)
    return entry

def getpwd(root):
    usr = StringVar()
    pwd = StringVar()
    root.title("username and password!")
    usrbox = makeentry(root, "User name:", 10, textvariable=usr)
    pwdbox = makeentry(root, "Password:", 10, show="*", textvariable=pwd)
    def onpwdentry(evt):
        root.destroy()
    def onokclick():
        root.destroy()
    usrbox.focus()
    pwdbox.bind('<Return>', onpwdentry)
    Button(root, command=onokclick, text = 'OK').pack(side = 'bottom')
    root.mainloop()
    return (usr.get(),pwd.get())
