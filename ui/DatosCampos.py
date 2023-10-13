# Form implementation generated from reading ui file 'DatosCampos.ui'
#
# Created by: PyQt6 UI code generator 6.4.2
#
# WARNING: Any manual changes made to this file will be lost when pyuic6 is
# run again.  Do not edit this file unless you know what you are doing.


from PyQt6 import QtCore, QtGui, QtWidgets


class Ui_Dialog(object):
    def setupUi(self, Dialog):
        Dialog.setObjectName("Dialog")
        Dialog.resize(712, 452)
        sizePolicy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.Policy.Fixed, QtWidgets.QSizePolicy.Policy.Fixed)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(Dialog.sizePolicy().hasHeightForWidth())
        Dialog.setSizePolicy(sizePolicy)
        Dialog.setMinimumSize(QtCore.QSize(476, 331))
        icon = QtGui.QIcon()
        icon.addPixmap(QtGui.QPixmap("Doc.ico"), QtGui.QIcon.Mode.Normal, QtGui.QIcon.State.Off)
        Dialog.setWindowIcon(icon)
        self.gridLayout = QtWidgets.QGridLayout(Dialog)
        self.gridLayout.setObjectName("gridLayout")
        self.lblTabla = QtWidgets.QLabel(parent=Dialog)
        palette = QtGui.QPalette()
        brush = QtGui.QBrush(QtGui.QColor(0, 85, 127))
        brush.setStyle(QtCore.Qt.BrushStyle.SolidPattern)
        palette.setBrush(QtGui.QPalette.ColorGroup.Active, QtGui.QPalette.ColorRole.WindowText, brush)
        brush = QtGui.QBrush(QtGui.QColor(0, 85, 127))
        brush.setStyle(QtCore.Qt.BrushStyle.SolidPattern)
        palette.setBrush(QtGui.QPalette.ColorGroup.Inactive, QtGui.QPalette.ColorRole.WindowText, brush)
        brush = QtGui.QBrush(QtGui.QColor(120, 120, 120))
        brush.setStyle(QtCore.Qt.BrushStyle.SolidPattern)
        palette.setBrush(QtGui.QPalette.ColorGroup.Disabled, QtGui.QPalette.ColorRole.WindowText, brush)
        self.lblTabla.setPalette(palette)
        font = QtGui.QFont()
        font.setPointSize(9)
        font.setBold(True)
        font.setWeight(75)
        self.lblTabla.setFont(font)
        self.lblTabla.setObjectName("lblTabla")
        self.gridLayout.addWidget(self.lblTabla, 0, 0, 1, 1)
        self.lblCampo = QtWidgets.QLabel(parent=Dialog)
        palette = QtGui.QPalette()
        brush = QtGui.QBrush(QtGui.QColor(0, 85, 127))
        brush.setStyle(QtCore.Qt.BrushStyle.SolidPattern)
        palette.setBrush(QtGui.QPalette.ColorGroup.Active, QtGui.QPalette.ColorRole.WindowText, brush)
        brush = QtGui.QBrush(QtGui.QColor(0, 85, 127))
        brush.setStyle(QtCore.Qt.BrushStyle.SolidPattern)
        palette.setBrush(QtGui.QPalette.ColorGroup.Inactive, QtGui.QPalette.ColorRole.WindowText, brush)
        brush = QtGui.QBrush(QtGui.QColor(120, 120, 120))
        brush.setStyle(QtCore.Qt.BrushStyle.SolidPattern)
        palette.setBrush(QtGui.QPalette.ColorGroup.Disabled, QtGui.QPalette.ColorRole.WindowText, brush)
        self.lblCampo.setPalette(palette)
        font = QtGui.QFont()
        font.setPointSize(9)
        font.setBold(True)
        font.setWeight(75)
        self.lblCampo.setFont(font)
        self.lblCampo.setObjectName("lblCampo")
        self.gridLayout.addWidget(self.lblCampo, 1, 0, 1, 1)
        self.lblKeyNameCampo = QtWidgets.QLabel(parent=Dialog)
        sizePolicy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.Policy.Preferred, QtWidgets.QSizePolicy.Policy.Preferred)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.lblKeyNameCampo.sizePolicy().hasHeightForWidth())
        self.lblKeyNameCampo.setSizePolicy(sizePolicy)
        self.lblKeyNameCampo.setObjectName("lblKeyNameCampo")
        self.gridLayout.addWidget(self.lblKeyNameCampo, 2, 0, 1, 1)
        self.txtKeyName = QtWidgets.QLineEdit(parent=Dialog)
        sizePolicy = QtWidgets.QSizePolicy(QtWidgets.QSizePolicy.Policy.Preferred, QtWidgets.QSizePolicy.Policy.Preferred)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.txtKeyName.sizePolicy().hasHeightForWidth())
        self.txtKeyName.setSizePolicy(sizePolicy)
        palette = QtGui.QPalette()
        brush = QtGui.QBrush(QtGui.QColor(0, 0, 255))
        brush.setStyle(QtCore.Qt.BrushStyle.SolidPattern)
        palette.setBrush(QtGui.QPalette.ColorGroup.Active, QtGui.QPalette.ColorRole.Text, brush)
        brush = QtGui.QBrush(QtGui.QColor(0, 0, 255, 128))
        brush.setStyle(QtCore.Qt.BrushStyle.NoBrush)
        palette.setBrush(QtGui.QPalette.ColorGroup.Active, QtGui.QPalette.ColorRole.PlaceholderText, brush)
        brush = QtGui.QBrush(QtGui.QColor(0, 0, 255))
        brush.setStyle(QtCore.Qt.BrushStyle.SolidPattern)
        palette.setBrush(QtGui.QPalette.ColorGroup.Inactive, QtGui.QPalette.ColorRole.Text, brush)
        brush = QtGui.QBrush(QtGui.QColor(0, 0, 255, 128))
        brush.setStyle(QtCore.Qt.BrushStyle.NoBrush)
        palette.setBrush(QtGui.QPalette.ColorGroup.Inactive, QtGui.QPalette.ColorRole.PlaceholderText, brush)
        brush = QtGui.QBrush(QtGui.QColor(120, 120, 120))
        brush.setStyle(QtCore.Qt.BrushStyle.SolidPattern)
        palette.setBrush(QtGui.QPalette.ColorGroup.Disabled, QtGui.QPalette.ColorRole.Text, brush)
        brush = QtGui.QBrush(QtGui.QColor(0, 0, 255, 128))
        brush.setStyle(QtCore.Qt.BrushStyle.NoBrush)
        palette.setBrush(QtGui.QPalette.ColorGroup.Disabled, QtGui.QPalette.ColorRole.PlaceholderText, brush)
        self.txtKeyName.setPalette(palette)
        font = QtGui.QFont()
        font.setFamily("Segoe UI")
        font.setPointSize(8)
        self.txtKeyName.setFont(font)
        self.txtKeyName.setObjectName("txtKeyName")
        self.gridLayout.addWidget(self.txtKeyName, 3, 0, 1, 3)
        self.lblComentariosCampo = QtWidgets.QLabel(parent=Dialog)
        self.lblComentariosCampo.setObjectName("lblComentariosCampo")
        self.gridLayout.addWidget(self.lblComentariosCampo, 4, 0, 1, 1)
        self.pteComentariosCampo = QtWidgets.QPlainTextEdit(parent=Dialog)
        palette = QtGui.QPalette()
        brush = QtGui.QBrush(QtGui.QColor(0, 0, 255))
        brush.setStyle(QtCore.Qt.BrushStyle.SolidPattern)
        palette.setBrush(QtGui.QPalette.ColorGroup.Active, QtGui.QPalette.ColorRole.Text, brush)
        brush = QtGui.QBrush(QtGui.QColor(0, 0, 255, 128))
        brush.setStyle(QtCore.Qt.BrushStyle.NoBrush)
        palette.setBrush(QtGui.QPalette.ColorGroup.Active, QtGui.QPalette.ColorRole.PlaceholderText, brush)
        brush = QtGui.QBrush(QtGui.QColor(0, 0, 255))
        brush.setStyle(QtCore.Qt.BrushStyle.SolidPattern)
        palette.setBrush(QtGui.QPalette.ColorGroup.Inactive, QtGui.QPalette.ColorRole.Text, brush)
        brush = QtGui.QBrush(QtGui.QColor(0, 0, 255, 128))
        brush.setStyle(QtCore.Qt.BrushStyle.NoBrush)
        palette.setBrush(QtGui.QPalette.ColorGroup.Inactive, QtGui.QPalette.ColorRole.PlaceholderText, brush)
        brush = QtGui.QBrush(QtGui.QColor(120, 120, 120))
        brush.setStyle(QtCore.Qt.BrushStyle.SolidPattern)
        palette.setBrush(QtGui.QPalette.ColorGroup.Disabled, QtGui.QPalette.ColorRole.Text, brush)
        brush = QtGui.QBrush(QtGui.QColor(0, 0, 255, 128))
        brush.setStyle(QtCore.Qt.BrushStyle.NoBrush)
        palette.setBrush(QtGui.QPalette.ColorGroup.Disabled, QtGui.QPalette.ColorRole.PlaceholderText, brush)
        self.pteComentariosCampo.setPalette(palette)
        font = QtGui.QFont()
        font.setFamily("Segoe UI")
        font.setPointSize(8)
        self.pteComentariosCampo.setFont(font)
        self.pteComentariosCampo.setObjectName("pteComentariosCampo")
        self.gridLayout.addWidget(self.pteComentariosCampo, 5, 0, 1, 3)
        spacerItem = QtWidgets.QSpacerItem(529, 20, QtWidgets.QSizePolicy.Policy.Expanding, QtWidgets.QSizePolicy.Policy.Minimum)
        self.gridLayout.addItem(spacerItem, 6, 0, 1, 1)
        self.btnCancel = QtWidgets.QPushButton(parent=Dialog)
        self.btnCancel.setObjectName("btnCancel")
        self.gridLayout.addWidget(self.btnCancel, 6, 1, 1, 1)
        self.btnOk = QtWidgets.QPushButton(parent=Dialog)
        self.btnOk.setDefault(True)
        self.btnOk.setObjectName("btnOk")
        self.gridLayout.addWidget(self.btnOk, 6, 2, 1, 1)
        self.lblKeyNameCampo.setBuddy(self.txtKeyName)
        self.lblComentariosCampo.setBuddy(self.pteComentariosCampo)

        self.retranslateUi(Dialog)
        self.btnCancel.clicked.connect(Dialog.close) # type: ignore
        QtCore.QMetaObject.connectSlotsByName(Dialog)

    def retranslateUi(self, Dialog):
        _translate = QtCore.QCoreApplication.translate
        Dialog.setWindowTitle(_translate("Dialog", "Datos del campo"))
        self.lblTabla.setText(_translate("Dialog", "Tabla: "))
        self.lblCampo.setText(_translate("Dialog", "Campo: "))
        self.lblKeyNameCampo.setText(_translate("Dialog", "Key &Name del Campo:"))
        self.lblComentariosCampo.setText(_translate("Dialog", "Co&mentarios del Campo:"))
        self.btnCancel.setText(_translate("Dialog", "&Cancelar"))
        self.btnOk.setText(_translate("Dialog", "&Aceptar"))
