from django import forms
from datetime import datetime

class LoanConvertForm(forms.Form):
    loan = forms.CharField(required=True,label='Номер договора', max_length=50,widget=forms.TextInput(attrs={'placeholder':'договор'}))
    #rate = forms.CharField(required=True,label='Курс конвертации', max_length=20,widget=forms.TextInput(attrs={'autocomplete': 'off','pattern':'[0-9]+([\.,][0-9]+)?', 'title':'Десятичное значение с не более чем 2-мя знаками в дробной части','placeholder':'курс'}))
    rate = forms.DecimalField(required=True,max_digits=5,decimal_places=2)
    date = forms.DateField(required=True,label='Дата конвертации',initial=datetime.today().strftime('%Y-%m-%d'),widget=forms.SelectDateWidget)

