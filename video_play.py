import numpy as np
import cv2

cap = cv2.VideoCapture('aws\\VID_20190512_231642.mp4')
fourcc = cv2.VideoWriter_fourcc(*'X264')
out = cv2.VideoWriter('output.mkv', fourcc, 20.0, (720,1280))

while(cap.isOpened()):
    ret,frame = cap.read()
    if not ret:
        print('no input data')
        break
    #frame = cv2.cvtColor(frame,cv2.COLOR_BGR2GRAY)
    cv2.imshow('frame',frame)
    out.write(frame)
    if cv2.waitKey(1) & 0xFF == ord('q'):
        break
cap.release()
out.release()
cv2.destroyAllWindows()
