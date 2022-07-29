import tweepy
from tweepy import Stream
from tweepy import OAuthHandler
import socket
import json

consumer_key='ztydxlKmmXE3H87csqg9a7pvs'
consumer_secret='fFoMPDam31UAD0c7mGKvWug9B8ieDvJKmSmu9i698yKFxlfeub'
access_token ='1258359758207062016-A4qzYBuBNW9GaBHpjSmmq8GTW0gD1P'
access_secret='Xss4vnE2jBVM0J88XhZMGgjbZZkEexQil4eeNOqzEwUi7'

class TweetsListener(Stream):
    
    def __init__(self, *args, csocket):
        super().__init__(*args)
        self.client_socket = csocket
    
    def on_data(self, data):
        try:  
            msg = json.loads( data )
            if "extended_tweet" in msg:
                self.client_socket\
                    .send(str(msg['extended_tweet']['full_text']+"t_end")\
                    .encode('utf-8'))         
                print(msg['extended_tweet']['full_text'])
            else:
                self.client_socket\
                    .send(str(msg['text']+"t_end")\
                    .encode('utf-8'))
                print(msg['text'])
            print('---------------------------------------------------------------------')
            return True
        except BaseException as e:
            print("Error on_data: %s" % str(e))
        return True
    def on_error(self, status):
        print(status)
        return True

def sendData(c_socket):
    twitter_stream = TweetsListener(consumer_key, consumer_secret, access_token, access_secret, csocket= c_socket)
    twitter_stream.filter(track = 'covid', languages=["en"])

if __name__ == "__main__":
    s = socket.socket()
    host = "127.0.0.1"    
    port = 5555
    s.bind((host, port))
    print('socket is ready')
    s.listen(4)
    print('socket is listening')
    c_socket, addr = s.accept()
    print("Received request from: " + str(addr))
    sendData(c_socket)