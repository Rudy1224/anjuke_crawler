import requests

def geocoding(address, city=None):
    headers = {'Referer':'http://developer.baidu.com'}
    payload = {'address':address, 'output':'json', 'ak':'5prliyM2Fydg24f3OhitqRvg'}
    if city!=None:
        payload['city']=city
    r = requests.get('http://api.map.baidu.com/geocoder/v2/', params=payload, headers=headers)
    if r.json()['status']==0:
        location = r.json()['result']['location']
        print(location)#['lng'], location['lat'])
    else: print(r.json()['msg'])

if __name__ == '__main__':
    geocoding('仁恒河滨城（一至三期）')