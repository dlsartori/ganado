from socket import AF_INET, SOCK_DGRAM
import sys
import ntplib
import socket
import struct
import time
import datetime

__servers = ["pool.ntp.org"]            #  "time.nist.gov" este no funca para tomar el tiempo.

def getNTPTime(host="pool.ntp.org"):
    port = 123
    buf = 1024
    address = (host, port)
    msg = '\x1b' + 47 * '\0'

    # reference time (in seconds since 1900-01-01 00:00:00)
    TIME1970 = 2208988800  # 1970-01-01 00:00:00
    # connect to server
    client = socket.socket(AF_INET, SOCK_DGRAM)
    # Esta linea abajo genera error si no hay conexion a internet (socket.gaierror: [Errno 11001] getaddrinfo failed)
    client.sendto(msg.encode('utf-8'), address)
    msg, address = client.recvfrom(buf)
    t = struct.unpack("!12I", msg)[10]
    t -= TIME1970
    # retValue = time.ctime(t).replace("  ", " ")
    retValue = [datetime.datetime.fromtimestamp(t), float(t)]
    return retValue


def getNTPDateTime(*, server=None):
    ntpDate = None
    try:
        client = ntplib.NTPClient()
        t_from_time = time.time()                           # These 2 are about equal
        # Esta linea genera error si no hay conexion a internet (socket.gaierror: [Errno 11001] getaddrinfo failed)
        response = client.request(server, version=4)        # These 2 are about equal
        print(f'response.tx_time: {response.tx_time}, {datetime.datetime.fromtimestamp(response.tx_time)}/ '
              f'time:{t_from_time} / utc_time: {datetime.datetime.utcfromtimestamp(response.tx_time)}')
        ntpDate = time.ctime(response.tx_time)
        print(ntpDate)
    except Exception as e:
        print(e)
        return e
    return datetime.datetime.strptime(ntpDate, "%a %b %d %H:%M:%S %Y")

if __name__ == "__main__":
    print(f'getNTPTime: {getNTPTime()}')
    print(f'getNTPDateTime: {getNTPDateTime(server="pool.ntp.org")}')
    # exit()
