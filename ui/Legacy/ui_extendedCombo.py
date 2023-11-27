#!/usr/bin/env python
# -*- coding: utf-8 -*-
# https://stackoverflow.com/questions/4827207/how-do-i-filter-the-pyqt-qcombobox-items-based-on-the-text-input
from PyQt6 import QtCore, QtGui, QtWidgets
from PyQt6.QtCore import Qt, QSortFilterProxyModel
from PyQt6.QtWidgets import QCompleter, QComboBox


class ExtendedComboBox(QComboBox):
    def __init__(self, parent=None):
        super(ExtendedComboBox, self).__init__(parent)

        # self.setFocusPolicy(Qt.StrongFocus)   # PyQt5
        self.setFocusPolicy(Qt.FocusPolicy.StrongFocus)
        self.setEditable(True)

        # add a filter model to filter matching items
        self.pFilterModel = QSortFilterProxyModel(self)
        # self.pFilterModel.setFilterCaseSensitivity(Qt.CaseInsensitive)   # PyQt5
        self.pFilterModel.setFilterCaseSensitivity(Qt.CaseSensitivity.CaseInsensitive)
        self.pFilterModel.setSourceModel(self.model())

        # add a completer, which uses the filter model
        self.completer = QCompleter(self.pFilterModel, self)
        # always show all (filtered) completions
        # self.completer.setCompletionMode(QCompleter.UnfilteredPopupCompletion)   # PyQt5
        self.completer.setCompletionMode(QCompleter.CompletionMode.UnfilteredPopupCompletion)
        self.setCompleter(self.completer)

        # connect signals
        self.lineEdit().textEdited.connect(self.pFilterModel.setFilterFixedString)
        self.completer.activated.connect(self.on_completer_activated)
        self.lineEdit().textChanged[str].connect(self.on_text_changed)

    def on_text_changed(self, text):
        print("Kk")

    # on selection of an item from the completer, select the corresponding item from combobox
    def on_completer_activated(self, text):
        if text:
            index = self.findText(text)
            self.setCurrentIndex(index)
            # self.activated[str].emit(self.itemText(index))   # PyQt5
            self.activated[int].emit(index)

    # on model change, update the models of the filter and completer as well
    def setModel(self, model):
        super(ExtendedComboBox, self).setModel(model)
        self.pFilterModel.setSourceModel(model)
        self.completer.setModel(self.pFilterModel)

    # on model column change, update the model column of the filter and completer as well
    def setModelColumn(self, column):
        self.completer.setCompletionColumn(column)
        self.pFilterModel.setFilterKeyColumn(column)
        super(ExtendedComboBox, self).setModelColumn(column)


if __name__ == "__main__":
    import sys
    from PyQt6.QtWidgets import QApplication
    from PyQt6.QtCore import QStringListModel

    app = QApplication(sys.argv)

    string_list = ['hola muchachos', 'adios amigos', 'hello world', 'good bye']

    combo = ExtendedComboBox()

    # either fill the standard model of the combobox
    combo.addItems(string_list)

    # or use another model
    #combo.setModel(QStringListModel(string_list))

    combo.resize(150, 20)
    combo.show()

    sys.exit(app.exec())
