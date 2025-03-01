import sys
from PyQt6 import QtCore, QtWidgets
from ui.IngresoCaravanas import Ui_dlgIngresoCaravanas
import krnl_db_query                    # Decorators for read_sql_query,
from krnl_tag import Tag
import pandas as pd
import numpy as np
from krnl_db_query import SQLiteQuery
from krnl_custom_types import getRecords, getrecords
from krnl_abstract_class_animal import Animal
from krnl_tag_tech_classes import *
from krnl_bovine import Bovine
import krnl_tag_animal


class UiAltaCaravanas():

    def __init__(self, parent=None):

        dlgAlta = DialogAltaCaravanas()
        dlgAlta.txtId.setFocus()
        if dlgAlta.exec() == QtWidgets.QDialog.DialogCode.Accepted:

            # set result variables
            dlgAlta.strCaravana = dlgAlta.txtId.text()
            dlgAlta.strComentario = dlgAlta.pteComentario.toPlainText()
            dlgAlta.intTipo = dlgAlta.cboTipo.currentData()
            dlgAlta.intColor = dlgAlta.cboColor.currentData()
            dlgAlta.intFormato = dlgAlta.cboFormato.currentData()
            dlgAlta.strTecnologia = dlgAlta.cboTecnologia.currentText() 

            # tech_dict = Tag.getTagTechClasses()

            dicto = {'fldFK_TagTechnology': str(dlgAlta.strTecnologia),
                     'fldFK_Color': dlgAlta.intColor,
                     'fldTagNumber': dlgAlta.strCaravana,
                     'fldFK_TagType': dlgAlta.intTipo,
                     'fldFK_TagFormat': dlgAlta.intFormato,
                     'fldComment': dlgAlta.strComentario}

            self.tag_obj = dlgAlta.tag_class.alta(**dicto)

            # TEMP
            print('Alta Caravanas:')
            print('    strCaravana: ' + dlgAlta.strCaravana)
            print('    strComentario: ' + dlgAlta.strComentario)
            print('    intTipo: ' + str(dlgAlta.intTipo))
            print('    intColor: ' + str(dlgAlta.intColor))
            print('    intFormato: ' + str(dlgAlta.intFormato))
            print('    strTecnologia: ' + str(dlgAlta.strTecnologia))
            print('Accepted')
        else:
            print('Rejected')
            raise RuntimeError


class DialogAltaCaravanas(QtWidgets.QDialog, Ui_dlgIngresoCaravanas):

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setupUi(self)

        Tag._tech_class_register.get('standard')
        self.tag_class = Bovine._myTagClass()

        # initialize result variables
        self.strCaravana = ''
        self.strComentario = ''
        self.intTipo = 1
        self.intColor = 1
        self.intFormato = 2
        self.strTecnologia = 'standard'

        # create lists of values for combo boxes
        frame = pd.read_sql_query('SELECT "Tipo De Caravana", "ID_Tipo De Caravana" FROM "Caravanas Tipos"', SQLiteQuery().conn)
        self.lstTipo = list(zip(frame.fldTagType, frame.fldID))
        self.lstTipos = getrecords('tblCaravanasTipos','fldTagType', 'fldID')
        self.lstTipos = sorted(list(zip(self.lstTipos.fldTagType.values,  self.lstTipos.fldID.values)))
        # self.lstTipos.sort_values('fldTagType', inplace=True)
        self.lstColores = getrecords('tblColores', 'fldName', 'fldID')
        self.lstColores = sorted(list(zip(self.lstColores.fldName.values,  self.lstColores.fldID.values)))
        self.lstFormatos = getrecords('tblCaravanasFormato', 'fldTagFormat', 'fldID')
        self.lstFormatos = sorted(list(zip(self.lstFormatos.fldTagFormat.values,  self.lstFormatos.fldID.values)))
        self.lstTecnologias= getrecords('tblCaravanasTecnologia', 'fldTagTechnology', 'fldID')
        self.lstTecnologias = sorted(list(zip(self.lstTecnologias.fldTagTechnology.values,  self.lstTecnologias.fldID.values)))

        # populate combo boxes
        for item, intId in self.lstTipos:
            self.cboTipo.addItem(item, str(intId))
        for item, intId in self.lstColores:
            self.cboColor.addItem(item, str(intId))
        for item, intId in self.lstFormatos:
            self.cboFormato.addItem(item, str(intId))
        for item, intId in self.lstTecnologias:
            self.cboTecnologia.addItem(item, str(intId))
        self.cboFormato.setCurrentIndex(self.cboFormato.findText('Tarjeta'))
        self.cboTecnologia.setCurrentIndex(self.cboTecnologia.findText('standard'))

        # signals & slots
        self.btnOk.clicked.connect(self.accept)
        self.btnCancel.clicked.connect(self.reject)


if __name__ == '__main__':
    app = QtWidgets.QApplication(sys.argv)
    alta = UiAltaCaravanas()
    del alta
