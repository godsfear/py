from cx_Freeze import setup, Executable
import os
import sys

includes = []
include_files = [r"c:\Users\d_degtyaryov\AppData\Local\Programs\Python\Python36-32\DLLs\tcl86t.dll",
                 r"c:\Users\d_degtyaryov\AppData\Local\Programs\Python\Python36-32\DLLs\tk86t.dll"]
os.environ['TCL_LIBRARY'] = r'C:\Users\d_degtyaryov\AppData\Local\Programs\Python\Python36-32\tcl\tcl8.6'
os.environ['TK_LIBRARY'] = r'C:\Users\d_degtyaryov\AppData\Local\Programs\Python\Python36-32\tcl\tk8.6'

setup(name = "ex_excel" ,
      version = "1.0" ,
      description = "" ,
      options={"build_exe": {"includes": includes, "include_files": include_files}},
      executables = [Executable("ex_excel.py")])
