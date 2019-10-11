#!/python
import pyotp
import qrcode
import qrcode.image.svg

def main():
    """init_totp = pyotp.random_base32()
    print(init_totp)
    url = pyotp.totp.TOTP(init_totp).provisioning_uri("test@google.com", issuer_name="test")
    qr = qrcode.QRCode(version=1,error_correction=qrcode.constants.ERROR_CORRECT_L,box_size=10,border=4,)
    qr.add_data(url)
    qr.make(fit=True)
    img = qr.make_image(fill_color="black", back_color="white").save('qr.png')"""
    
    totp = pyotp.TOTP("WJ37KYPS6JEWOETQ")
    print(totp.now())

if __name__ == '__main__':
    main()
