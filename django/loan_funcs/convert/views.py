from django.http import HttpResponseRedirect
from django.shortcuts import render
from .forms import LoanConvertForm

import sys
sys.path.insert(0,'.\\utils\\')
from utils.xfuncs import str2dec
from utils.loanconv import lconv
from io import StringIO

# Create your views here.

def index(request):
    if request.method == 'POST':
        form = LoanConvertForm(request.POST)
        if form.is_valid():
            loan = form.cleaned_data['loan']
            rate = form.cleaned_data['rate']
            date = form.cleaned_data['date']
            
            old_stdout = sys.stdout
            sys.stdout = mystdout = StringIO()
            lconv('SECURITY',loan,str2dec(rate),date,False)
            output = mystdout.getvalue()
            sys.stdout = old_stdout
            
            return HttpResponseRedirect(output,content_type='text/plain')
    else:
        form = LoanConvertForm()

    return render(request, 'index.html', {'form': form})
