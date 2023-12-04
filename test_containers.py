from krnl_config import print, DISMISS_PRINT
from krnl_geo_new import Geo


if __name__ == '__main__':

    el_nandu = '42990b601cec4ddb9d85bfb94cda2e29'     # accents removed: 'el ñandu'--> OJO! removeAccents() keeps 'ñ'!!

    nandu = Geo.getGeoEntities()[el_nandu]
    Geo.getName('lote 1')
    lote1 = Geo.getGeoEntities()['979dce10c6064a35b3828f7e7abb181b']
    print(f'l1 __containers: {[j.name for j in lote1.containers]}')
    print(f'l1 containers: {[j.name for j in lote1.containers]}')
    print(nandu.contains(lote1))
    print(lote1.contained_in(nandu))
    print(f'LocalizLevelDict: {Geo.getLocalizLevelsDict()}')
    a = Geo.getObject('El Ñandu "lote -   %^1\\"')
    Geo.createGeoEntity(name='FeudoProvince', entity_type='Provincia',
                        containers=Geo.getGeoEntities()[Geo.getUID('argentina')],
                        abbrev='FEU', state_entity=True)

    # print(f'All Geo Objects: {Geo.getGeoEntities()}')
    feudalia = next((e for e in Geo.getGeoEntities().values() if 'feudo' in e.name.lower()), 'Entity not found')
    print_str = f'my Feudo name is: ' + (f'{feudalia.name}' if isinstance(feudalia, Geo) else feudalia)
    print(print_str)
    if isinstance(feudalia, Geo):
        feudalia.removeGeoEntity()


