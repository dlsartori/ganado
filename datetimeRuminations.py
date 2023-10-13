import datetime
from datetime import datetime

from krnl_cfg import *

if __name__ == '__main__':

    def dbCreateDT(strDT: str):
        """
        Creates datetime object from argument string COMING FROM A DB READ. Important differences with user input, as
        blank / NULLS are handled differently. To be used exclusively to access DB.
        @param strDT: String in format YYYY-MM-DD
        @return: Valid input string -> datetime object representing argument values
                 Non valid input string -> obj_dtError
                 Blank ('', 0 or None) -> obj_dtBlank
        """
        zeroPaddingTime = '00:00:00:000000'
        lenDT = 26
        lenT = 15

        if strDT == '' or strDT == 0 or strDT is None:  # Cualquier tipo de dato que no sea string lo convierte a ''
            return obj_dtBlank             # Objeto datetime que indica '' o None.
        else:                                # Esto es el createDTShort
            print(f'ORIGINAL: {strDT}')
            strDT = str(strDT).strip()
            if 0 < strDT.find(' ') < 10 or len(strDT) < lenDT:            # Procesa si se paso fecha "corta" (ej. 2020/1/1)
                strDate = strDT.replace('/', '-', strDT.count('/'))
                strDate = strDate.replace(' ', '-', 2-strDate.count('-'))
                strDate = strDate[:min(10, (strDate.find(" ")+1 if strDate.find(" ") > 0 else 10))]
                yr = strDate[:strDate.find('-')]
                mon = strDate[strDate.find('-')+1:strDate.find('-', strDate.find('-')+1)]
                day = strDate[strDate.find('-', strDate.find('-')+1)+1:len(strDate)]
                print(f'strDate: {strDate} / strYear: {yr} /  strMonth: {mon}  / strDay: {day} ')
                strTime = strDT[-(len(strDT) - len(strDate)):].strip() if len(strDate) != len(strDT) else zeroPaddingTime
                print(f'strTime INICIAL: {strTime} / len(strDT):{len(strDT)} / len(strDate):{len(strDate)}')
                strTime = strTime + zeroPaddingTime[-(lenT - len(strTime)):] if len(strTime) < lenT else strTime[:lenT]
                print(f'strTime: {strTime}')
                hour = strTime[:2]
                minut = strTime[3:5]
                sec = strTime[6:8]
                usec = strTime[9:]
                print(f'strTime: {strTime} / hour:{hour}; min:{minut}; sec:{sec}; usec:{usec}')
            else:                                   # Asume string formateado YYYY-MM-DD HH:MM:SS:NNNNNN
                strDT = strDT[:lenDT]
                yr = strDT[:4]
                mon = strDT[5:7]
                day = strDT[8:10]
                hour = strDT[11:13]
                minut = strDT[14:16]
                sec = strDT[17:19]
                usec = strDT[20:]
            try:
                yr = int(yr)
            except (TypeError, ValueError, NameError):
                return obj_dtError
            try:
                mon = int(mon)
            except (TypeError, ValueError, NameError):
                return obj_dtError
            try:
                day = int(day)
            except (TypeError, ValueError, NameError):
                return obj_dtError
            try:
                hour = int(hour)                             # min(int(hour), 23) Por ahora esto no va
            except (TypeError, ValueError, NameError):
                return obj_dtError
            try:
                minut = min(int(minut), 59)
            except (TypeError, ValueError, NameError):
                return obj_dtError
            try:
                sec = min(int(sec), 59)
            except (TypeError, ValueError, NameError):
                return obj_dtError
            try:
                usec = int(usec)
            except (TypeError, ValueError, NameError):
                return obj_dtError
            try:
                dt = datetime(yr, mon, day, hour, minut, sec, usec)
            except (TypeError, ValueError, NameError):
                dt = obj_dtError
        print(f'OBJECT dt is: {dt}')
        return dt

    dbCreateDT('202045terg 5-1 12:63')          # '2020-11-21 12:44:35:123456' / '2020 1-4 12:44' / '2020 5-1 12:63'





    def createDTNew1(strDT: str):
        """
        Creates datetime object from argument string, in the format fmt
        @param strDT: String in format YYYY-MM-DD
        @param fmt: string Format
        @return: if input string is valid -> datetime object with input string values
                 if input string is not valid -> "1" datetime object
                 if type(input) is NOT type string, is equal to '' or is equal to None -> "2" datetime object
        """
        zeroPaddingDT = '0001-01-01 00:00:00:000000'
        zeroPaddingDate = '0001-01-01'
        zeroPaddingTime = '00:00:00:000000'
        lenDT = 26
        lenT = 15
        if strDT == '' or strDT == 0 or strDT is None:  # Cualquier tipo de dato que no sea string lo convierte a ''
            return obj_dtBlank              # Objeto datetime que indica '' o None.
        elif type(strDT) is not str:
            return obj_dtError              # # Objeto datetime que indica fecha no Valida.
        else:                           # Esto es el createDTShort
            print(f'ORIGINAL: {strDT}')
            strDate = strDT.strip().replace('/', '-', strDT.count('/'))
            strDate = strDate.replace(' ', '-', 2-strDate.count('-'))
            strDate = strDate[:min(10, (strDate.find(" ")+1 if strDate.find(" ") > 0 else 10))]
            strYear = strDate[:strDate.find('-')]
            strMon = strDate[strDate.find('-')+1:strDate.find('-', strDate.find('-')+1)]
            strDay = strDate[strDate.find('-', strDate.find('-')+1)+1:len(strDate)]
            strTime = strDT[-(len(strDT)-len(strDate)):]
            strTime = strTime + zeroPaddingTime[-(15 - len(strTime)):] if len(strTime) < 15 else strTime[:15]
            strHour = strTime[:strTime.find(':')]

        print(f'strDate: {strDate} / strYear: {strYear} /  strMonth: {strMon}  / strDay: {strDay} ')
        print(f'strTime: {strTime} / strHour: {strHour} / strMin: {444} /  strSec: {555} ')
        try: yr = int(strYear)
        except (TypeError, ValueError, NameError): yr = -1
        try: mon = int(strMon)
        except (TypeError, ValueError, NameError): mon = -1
        try: day = int(strDay)
        except (TypeError, ValueError, NameError): day = -1
        try: hr = int(strDT[-16:-13:])
        except (TypeError, ValueError, NameError): hr = -1
        try: minut = int(strDT[14:16])
        except (TypeError, ValueError, NameError): minut = -1
        try: sec = int(strDT[17:19])
        except (TypeError, ValueError, NameError): sec = -1
        try: usec = int(strDT[20:26])
        except (TypeError, ValueError, NameError): usec = -1
        print(f'year: {yr} - mon: {mon} - day: {day} // hr: {hr} - min: {minut} - sec: {sec} - usec: {usec}')

        try:
            dt = datetime(yr, mon, day, hr, minut, sec, usec)
        except (TypeError, ValueError, NameError):
            dt = 'INFO_Inp_InvalidDateInvalidDate'
            retValue = False
        print(f'...and datetime is: {dt}')


def createDTOriginal(strDT: str, fmt=fDateTime):
        """
        Creates datetime object from argument string, in the format fmt
        @param strDT: String in format YYYY-MM-DD
        @param fmt: string Format
        @return: if input string is valid -> datetime object with input string values
                 if input string is not valid -> "1" datetime object
                 if type(input) is NOT type string, is equal to '' or is equal to None -> "2" datetime object
        """
        zeroPaddingDT = '0001-01-01 00:00:00:000000'
        zeroPaddingDate = '0001-01-01'
        lenDT = 26
        lenT = 15
        if strDT == '' or type(strDT) is not str:  # Cualquier tipo de dato que no sea string lo convierte a ''
            strDT = '---'             # datetime.strptime(str_dtBlank, fDate)  # str_dwTwo: Objeto datetime que indica '' o None.
        else:
            strDT = str(strDT).strip()
            if fmt == fDateTime:  # Right zero-padding para completar string a formato fDateTime
                strDT = strDT + zeroPaddingDT[-(lenDT - len(strDT)):] if len(strDT) < lenDT else strDT[:lenDT]
            elif fmt == fDate:  # Trunca string a formato fDate
                strDT = strDT[:10] if len(strDT) >= 10 else strDT + zeroPaddingDate[-(10 - len(strDT)):]
            elif fmt == fTime:  # Trunca string a formato fTime. OJO!!!-> RETORNA: 0001-01-01 HH:MM:SS:000000 (objeto datetime)
                if strDT.count(' ') > 0:
                    strDT = (strDT[-(len(strDT) - (strDT.find(' ') + strDT.count(' '))):])[:lenT] if len(strDT) >= lenT \
                        else strDT + zeroPaddingDT[-(lenT - len(strDT)):]
                else:
                    strDT = strDT + zeroPaddingDT[-(lenT - len(strDT)):] if len(strDT) < lenT else strDT[:lenT]
            else:
                strDT = strDT + zeroPaddingDT[-(lenDT - len(strDT)):] if len(strDT) < lenDT else strDT[:lenDT]
        print(f'strDT: {strDT}')

        # try:
        #     retValue = datetime.strptime(stringDT, fmt)
        # except (TypeError, ValueError, IndexError, AttributeError, NameError):
        #     retValue = datetime.strptime(str_dtError, fDate)  # retValue = datetime.strptime(str_dtError, fDate)
        #     print(f'ERR_UI_InvalidArgument - CreateDT(): Date Formatting Error')
        # return retValue
