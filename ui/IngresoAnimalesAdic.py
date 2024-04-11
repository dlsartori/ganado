# Form implementation generated from reading ui file 'IngresoAnimalesAdic.ui'
#
# Created by: PyQt6 UI code generator 6.6.1
#
# WARNING: Any manual changes made to this file will be lost when pyuic6 is
# run again.  Do not edit this file unless you know what you are doing.


from PyQt6 import QtCore, QtGui, QtWidgets


class Ui_dlgIngresoAnimalesAdic(object):
    def setupUi(self, dlgIngresoAnimalesAdic):
        dlgIngresoAnimalesAdic.setObjectName("dlgIngresoAnimalesAdic")
        dlgIngresoAnimalesAdic.setEnabled(True)
        dlgIngresoAnimalesAdic.resize(466, 202)
        dlgIngresoAnimalesAdic.setBaseSize(QtCore.QSize(7, 0))
        icon = QtGui.QIcon()
        icon.addPixmap(QtGui.QPixmap("Ganado.ico"), QtGui.QIcon.Mode.Normal, QtGui.QIcon.State.Off)
        dlgIngresoAnimalesAdic.setWindowIcon(icon)
        dlgIngresoAnimalesAdic.setAutoFillBackground(False)
        dlgIngresoAnimalesAdic.setInputMethodHints(QtCore.Qt.InputMethodHint.ImhPreferNumbers)
        self.btnCancel = QtWidgets.QPushButton(parent=dlgIngresoAnimalesAdic)
        self.btnCancel.setGeometry(QtCore.QRect(290, 170, 76, 23))
        self.btnCancel.setObjectName("btnCancel")
        self.btnOk = QtWidgets.QPushButton(parent=dlgIngresoAnimalesAdic)
        self.btnOk.setGeometry(QtCore.QRect(375, 170, 76, 23))
        self.btnOk.setDefault(True)
        self.btnOk.setObjectName("btnOk")
        self.grpEdad = QtWidgets.QGroupBox(parent=dlgIngresoAnimalesAdic)
        self.grpEdad.setEnabled(True)
        self.grpEdad.setGeometry(QtCore.QRect(15, 10, 226, 76))
        self.grpEdad.setObjectName("grpEdad")
        self.datNacimiento = QtWidgets.QDateEdit(parent=self.grpEdad)
        self.datNacimiento.setGeometry(QtCore.QRect(140, 20, 76, 20))
        self.datNacimiento.setDateTime(QtCore.QDateTime(QtCore.QDate(2021, 1, 1), QtCore.QTime(0, 0, 0)))
        self.datNacimiento.setCalendarPopup(True)
        self.datNacimiento.setObjectName("datNacimiento")
        self.radFechaNacimiento = QtWidgets.QRadioButton(parent=self.grpEdad)
        self.radFechaNacimiento.setGeometry(QtCore.QRect(15, 22, 121, 17))
        self.radFechaNacimiento.setChecked(True)
        self.radFechaNacimiento.setObjectName("radFechaNacimiento")
        self.radEdad = QtWidgets.QRadioButton(parent=self.grpEdad)
        self.radEdad.setGeometry(QtCore.QRect(15, 45, 96, 17))
        self.radEdad.setObjectName("radEdad")
        self.dspEdad = QtWidgets.QDoubleSpinBox(parent=self.grpEdad)
        self.dspEdad.setEnabled(False)
        self.dspEdad.setGeometry(QtCore.QRect(140, 45, 76, 20))
        self.dspEdad.setDecimals(1)
        self.dspEdad.setObjectName("dspEdad")
        self.lblPeso = QtWidgets.QLabel(parent=dlgIngresoAnimalesAdic)
        self.lblPeso.setGeometry(QtCore.QRect(62, 143, 90, 13))
        self.lblPeso.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight|QtCore.Qt.AlignmentFlag.AlignTrailing|QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.lblPeso.setObjectName("lblPeso")
        self.grpUbicacion = QtWidgets.QGroupBox(parent=dlgIngresoAnimalesAdic)
        self.grpUbicacion.setEnabled(True)
        self.grpUbicacion.setGeometry(QtCore.QRect(250, 10, 201, 106))
        self.grpUbicacion.setObjectName("grpUbicacion")
        self.cboPotrero = ComboTabla(parent=self.grpUbicacion)
        self.cboPotrero.setEnabled(True)
        self.cboPotrero.setGeometry(QtCore.QRect(70, 47, 121, 20))
        self.cboPotrero.setObjectName("cboPotrero")
        self.lblLote = QtWidgets.QLabel(parent=self.grpUbicacion)
        self.lblLote.setGeometry(QtCore.QRect(20, 22, 46, 13))
        self.lblLote.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight|QtCore.Qt.AlignmentFlag.AlignTrailing|QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.lblLote.setObjectName("lblLote")
        self.cboLote = ComboTabla(parent=self.grpUbicacion)
        self.cboLote.setEnabled(True)
        self.cboLote.setGeometry(QtCore.QRect(70, 20, 121, 20))
        self.cboLote.setObjectName("cboLote")
        self.lblPotrero = QtWidgets.QLabel(parent=self.grpUbicacion)
        self.lblPotrero.setGeometry(QtCore.QRect(20, 48, 46, 13))
        self.lblPotrero.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight|QtCore.Qt.AlignmentFlag.AlignTrailing|QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.lblPotrero.setObjectName("lblPotrero")
        self.cboLocacion = ComboTabla(parent=self.grpUbicacion)
        self.cboLocacion.setEnabled(True)
        self.cboLocacion.setGeometry(QtCore.QRect(70, 75, 121, 20))
        self.cboLocacion.setObjectName("cboLocacion")
        self.lblLocacion = QtWidgets.QLabel(parent=self.grpUbicacion)
        self.lblLocacion.setGeometry(QtCore.QRect(15, 75, 51, 20))
        self.lblLocacion.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight|QtCore.Qt.AlignmentFlag.AlignTrailing|QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.lblLocacion.setObjectName("lblLocacion")
        self.dspPeso = QtWidgets.QDoubleSpinBox(parent=dlgIngresoAnimalesAdic)
        self.dspPeso.setEnabled(True)
        self.dspPeso.setGeometry(QtCore.QRect(156, 140, 76, 20))
        self.dspPeso.setButtonSymbols(QtWidgets.QAbstractSpinBox.ButtonSymbols.NoButtons)
        self.dspPeso.setDecimals(1)
        self.dspPeso.setMinimum(50.0)
        self.dspPeso.setMaximum(999.0)
        self.dspPeso.setObjectName("dspPeso")
        self.datServicio = QtWidgets.QDateEdit(parent=dlgIngresoAnimalesAdic)
        self.datServicio.setGeometry(QtCore.QRect(156, 115, 76, 20))
        self.datServicio.setDateTime(QtCore.QDateTime(QtCore.QDate(2021, 1, 1), QtCore.QTime(0, 0, 0)))
        self.datServicio.setCalendarPopup(True)
        self.datServicio.setObjectName("datServicio")
        self.lblFechaServicio = QtWidgets.QLabel(parent=dlgIngresoAnimalesAdic)
        self.lblFechaServicio.setGeometry(QtCore.QRect(40, 115, 111, 16))
        self.lblFechaServicio.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight|QtCore.Qt.AlignmentFlag.AlignTrailing|QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.lblFechaServicio.setObjectName("lblFechaServicio")
        self.datDestete = QtWidgets.QDateEdit(parent=dlgIngresoAnimalesAdic)
        self.datDestete.setGeometry(QtCore.QRect(156, 90, 76, 20))
        self.datDestete.setDateTime(QtCore.QDateTime(QtCore.QDate(2021, 1, 1), QtCore.QTime(0, 0, 0)))
        self.datDestete.setCalendarPopup(True)
        self.datDestete.setObjectName("datDestete")
        self.lblFechaDestete = QtWidgets.QLabel(parent=dlgIngresoAnimalesAdic)
        self.lblFechaDestete.setGeometry(QtCore.QRect(40, 90, 111, 16))
        self.lblFechaDestete.setAlignment(QtCore.Qt.AlignmentFlag.AlignRight|QtCore.Qt.AlignmentFlag.AlignTrailing|QtCore.Qt.AlignmentFlag.AlignVCenter)
        self.lblFechaDestete.setObjectName("lblFechaDestete")
        self.chkTratamientos = QtWidgets.QCheckBox(parent=dlgIngresoAnimalesAdic)
        self.chkTratamientos.setGeometry(QtCore.QRect(260, 120, 166, 17))
        self.chkTratamientos.setObjectName("chkTratamientos")
        self.chkDieta = QtWidgets.QCheckBox(parent=dlgIngresoAnimalesAdic)
        self.chkDieta.setGeometry(QtCore.QRect(260, 140, 166, 17))
        self.chkDieta.setObjectName("chkDieta")
        self.lblPeso.setBuddy(self.dspPeso)
        self.lblLote.setBuddy(self.cboLote)
        self.lblPotrero.setBuddy(self.cboPotrero)
        self.lblLocacion.setBuddy(self.cboLocacion)
        self.lblFechaServicio.setBuddy(self.datServicio)
        self.lblFechaDestete.setBuddy(self.datDestete)

        self.retranslateUi(dlgIngresoAnimalesAdic)
        self.radFechaNacimiento.toggled['bool'].connect(self.datNacimiento.setEnabled) # type: ignore
        self.radEdad.toggled['bool'].connect(self.dspEdad.setEnabled) # type: ignore
        QtCore.QMetaObject.connectSlotsByName(dlgIngresoAnimalesAdic)
        dlgIngresoAnimalesAdic.setTabOrder(self.radFechaNacimiento, self.radEdad)
        dlgIngresoAnimalesAdic.setTabOrder(self.radEdad, self.datNacimiento)
        dlgIngresoAnimalesAdic.setTabOrder(self.datNacimiento, self.dspEdad)
        dlgIngresoAnimalesAdic.setTabOrder(self.dspEdad, self.cboLote)
        dlgIngresoAnimalesAdic.setTabOrder(self.cboLote, self.cboPotrero)
        dlgIngresoAnimalesAdic.setTabOrder(self.cboPotrero, self.cboLocacion)
        dlgIngresoAnimalesAdic.setTabOrder(self.cboLocacion, self.datDestete)
        dlgIngresoAnimalesAdic.setTabOrder(self.datDestete, self.datServicio)
        dlgIngresoAnimalesAdic.setTabOrder(self.datServicio, self.dspPeso)
        dlgIngresoAnimalesAdic.setTabOrder(self.dspPeso, self.chkTratamientos)
        dlgIngresoAnimalesAdic.setTabOrder(self.chkTratamientos, self.chkDieta)
        dlgIngresoAnimalesAdic.setTabOrder(self.chkDieta, self.btnCancel)
        dlgIngresoAnimalesAdic.setTabOrder(self.btnCancel, self.btnOk)

    def retranslateUi(self, dlgIngresoAnimalesAdic):
        _translate = QtCore.QCoreApplication.translate
        dlgIngresoAnimalesAdic.setWindowTitle(_translate("dlgIngresoAnimalesAdic", "Datos Adicionales"))
        self.btnCancel.setText(_translate("dlgIngresoAnimalesAdic", "Cancelar"))
        self.btnOk.setText(_translate("dlgIngresoAnimalesAdic", "Aceptar"))
        self.grpEdad.setTitle(_translate("dlgIngresoAnimalesAdic", "Edad del Animal"))
        self.datNacimiento.setDisplayFormat(_translate("dlgIngresoAnimalesAdic", "d/M/yyyy"))
        self.radFechaNacimiento.setText(_translate("dlgIngresoAnimalesAdic", "&Fecha de Nacimiento"))
        self.radEdad.setText(_translate("dlgIngresoAnimalesAdic", "&Edad (años)"))
        self.lblPeso.setText(_translate("dlgIngresoAnimalesAdic", "Peso en &Kg."))
        self.grpUbicacion.setTitle(_translate("dlgIngresoAnimalesAdic", "Ubicación"))
        self.lblLote.setText(_translate("dlgIngresoAnimalesAdic", "&Lote"))
        self.lblPotrero.setText(_translate("dlgIngresoAnimalesAdic", "&Potrero"))
        self.lblLocacion.setText(_translate("dlgIngresoAnimalesAdic", "Loca&ción"))
        self.datServicio.setDisplayFormat(_translate("dlgIngresoAnimalesAdic", "d/M/yyyy"))
        self.lblFechaServicio.setText(_translate("dlgIngresoAnimalesAdic", "Entrada en &Servicio"))
        self.datDestete.setDisplayFormat(_translate("dlgIngresoAnimalesAdic", "d/M/yyyy"))
        self.lblFechaDestete.setText(_translate("dlgIngresoAnimalesAdic", "&Destete"))
        self.chkTratamientos.setText(_translate("dlgIngresoAnimalesAdic", "Asignar &Tratamientos"))
        self.chkDieta.setText(_translate("dlgIngresoAnimalesAdic", "Asignar D&ieta"))
from ui.comboTabla import ComboTabla
