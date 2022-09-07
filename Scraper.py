"""
Copyright (C) NEXTracker, Inc - All Rights Reserved
Unauthorized copying of this file, via any medium is strictly prohibited
Proprietary and confidential
"""
__author__ = "hvaranasi"

import configparser
import os
import tkinter as tk
from tkinter import scrolledtext, ttk, IntVar, LabelFrame
import threading
import Connector as con


class ConfigManager():
    # used if one isn't passed to the instance
    DEFAULT_DICT = {
        'Date': {'FROM_DATE': '2022-05-25T00:00:01Z', 'TO_DATE': '2022-06-24T00:00:01Z'},
        'PLANT_DETAILS': {'Site_ID': '1106'}, 'NCU_SPC_Details': {'NID': '57352', 'SPC': 'SPCMX20192707930'},
        'SERVER_DETAILS': {'API SERVER': "http://172.21.190.155:8899",
                           'BEARER TOKEN': "72dc6534-ea6e-4962-b089-9f21a222fd7"}}

    def __init__(self, configpath=None, _title='Data Scraping Tool', defaultdict=DEFAULT_DICT):

        """
        Initializes a new ConfigManager.
            * parent: should be a Tkinter object
            * configpath =  an absolute path to the config file
            * _title: optional, the Toplevel's title
            * defaultdict: dict to use when resetting defaults
             uses a demo dict for defaults if one isn't passed.
        """

        self.root = tk.Tk()
        self.root.minsize(550, 500)  # big enough to hold the DEFAULT_DICT
        self.window_title = _title  # title of the Toplevel
        self.defaultdict = defaultdict
        self.defaultdict = ConfigManager.DEFAULT_DICT

        if configpath is None:
            self.configpath = os.path.join(os.getcwd(), "config.ini")
        else:
            self.configpath = configpath

        # ------------------------Widget options-------------------------------
        # the string length threshold for using Entry/Text widget
        self.max_entry_length = 65
        self.text_width = 28  # width of text widgets
        self.text_height = 10  # height of text widgets
        self.entry_width = 40  # width of entry widgets
        self.label_width = 30  # width of label widgets
        # ---------------------------------------------------------------------

        # assess the parser dict
        parser = configparser.ConfigParser()
        try:
            parser.read(self.configpath)
        except TypeError as e:
            print(e)
            print("Using the defaultdict instead")
            parser.read_dict(self.defaultdict)
            # parser_dict = self.as_dict(parser)
            # self.build(parser_dict)
        finally:
            parser_dict = self.as_dict(parser)
            self.build(parser_dict)  # "parser_dict" contains the "config.ini" information

    def build(self, parser_dict: dict):

        """Dynamically populates GUI from the contents of parser_dict"""
        try:
            self.container.destroy()
        except AttributeError:
            print("GUI Loading...")

        self.parser_dict = parser_dict
        self._fields = []  # list of the input widgets from every section
        self._sections = self.parser_dict.keys()  # list of all the sections
        self._section_keys = []  # list of keys from every section
        for section in self._sections:
            self._section_keys.extend(self.parser_dict[section].keys())

        # ----------------------------- Cosmetics ---------------------------
        self.root.title(self.window_title)
        self.container = tk.Frame(self.root)
        self.canvas = tk.Canvas(self.container)
        self.scrollable_frame = tk.Frame(self.canvas)

        # make a LabelFrame for each section in the ConfigParser
        for section in self.parser_dict.keys():
            frm = tk.LabelFrame(self.scrollable_frame, text=section.title(), font=('courier', 12, 'bold'))
            frm.grid_columnconfigure(0, weight=1)
            #             frm.grid_columnconfigure(1, weight=1)
            # make a label and Entry/Text widget for each key in the section
            for idx, section_key in enumerate(self.parser_dict[section].keys()):
                self._section_keys.append(section_key)
                frm.grid_rowconfigure(idx, weight=1)

                tk.Label(frm, text=section_key.title(), anchor='ne', width=self.label_width).grid(row=idx, column=0,
                                                                                                  padx=2, pady=2,
                                                                                                  sticky='e')

                # the length of this particular value
                val_len = len(self.parser_dict[section][section_key])

                if val_len >= self.max_entry_length:  # if the key has a long value put it in a scrolledtext
                    ent = tk.scrolledtext.ScrolledText(frm, width=self.text_width, height=self.text_height)
                    ent.grid(row=idx, column=1, padx=2, pady=2, sticky='e')

                    the_key = self.parser_dict[section][section_key]
                    if ',' in the_key:  # check to see if the string is a list
                        the_key = the_key.split(',')
                        for word in (the_key):
                            ent.insert('end', word.strip())
                            if not word in the_key[-1]:
                                ent.insert('end', ',\n')
                    else:  # it isn't clearly a list, so just stick it in there
                        ent.insert(1.0, the_key.strip())
                # use an entry widget if the key's value is short enough
                else:
                    ent = ttk.Entry(frm, width=self.entry_width)
                    ent.grid(row=idx, column=1, padx=2, pady=2, sticky='e')
                    ent.insert(0, self.parser_dict[section][section_key])
                # after deciding which to make, add to a list for convenience
                self._fields.append(ent)
            # finally, pack the LabelFrame for that section
            frm.pack(pady=5, padx=5, anchor='sw', fill='both', expand=True)

        #####################################################################
        '''
        Checkboxes to select NCU or SPC information.
        '''
        top = LabelFrame(self.scrollable_frame, text="Select To Download", font=('courier', 12, 'bold'))
        top.pack(pady=5, padx=5, anchor='sw', fill='both', expand=True)
        self.CheckVar1 = IntVar()
        self.CheckVar2 = IntVar()
        self.CheckVar3 = IntVar()

        C3 = ttk.Checkbutton(top, text="GHI", variable=self.CheckVar3, onvalue=1, offvalue=0)
        C3.pack(side='right', anchor='se', expand='YES')
        C1 = ttk.Checkbutton(top, text="NCU", variable=self.CheckVar1, onvalue=1, offvalue=0)
        C1.pack(side='right', anchor='se', expand='YES')
        C2 = ttk.Checkbutton(top, text="SPC", variable=self.CheckVar2, onvalue=1, offvalue=0)
        C2.pack(side='right', anchor='se', expand='YES')
        #####################################################################

        '''
        Checkboxes to select NCU or SPC information.
        '''
        low = LabelFrame(self.scrollable_frame, text="Resample NCU & GHI Data By", font=('courier', 12, 'bold'))
        low.pack(pady=5, padx=5, anchor='sw', fill='both', expand=True)
        self.Chk5min = IntVar()
        self.Chk10min = IntVar()
        self.Chk15min = IntVar()

        five_min = ttk.Checkbutton(low, text="5 Min", variable=self.Chk5min, onvalue=1, offvalue=0)
        five_min.pack(side='right', anchor='se', expand='YES')
        ten_min = ttk.Checkbutton(low, text="10 Min", variable=self.Chk10min, onvalue=1, offvalue=0)
        ten_min.pack(side='right', anchor='se', expand='YES')
        fifteen_min = ttk.Checkbutton(low, text="15 Min", variable=self.Chk15min, onvalue=1, offvalue=0)
        fifteen_min.pack(side='right', anchor='se', expand='YES')

        #####################################################################
        # make buttons to save the form or reset to defaults
        btnfrm1 = tk.Frame(self.scrollable_frame)
        ttk.Button(btnfrm1, text='Reset Defaults', command=lambda: self.reset_config()).grid(row=0, column=0, pady=5,
                                                                                             padx=6, sticky='nw')
        btnfrm1.pack(side=tk.LEFT)  # put them at the bottom of the form

        # Apply and Execute Button
        btnfrm1 = tk.Frame(self.scrollable_frame)
        ttk.Button(btnfrm1, text='Apply', command=lambda: self.apply_config()).grid(row=0, column=0, pady=5, padx=3,
                                                                                    sticky='nw')
        ttk.Button(btnfrm1, text='Execute', command=threading.Thread(target=lambda: self.execute_config()).start).grid(
            row=0, column=1, pady=5,
            padx=3, sticky='nw')
        ttk.Button(btnfrm1, text='Kill', command=lambda: self.destroy()).grid(row=0, column=2, pady=5,
                                                                              padx=3, sticky='nw')
        btnfrm1.pack(side=tk.RIGHT)  # put them at the bottom of the form

        # # *******************************************************************************************************
        # ttk.Progressbar(btnfrm1, orient='horizontal', length=300).grid(row=1, column=0, pady=5, padx=0)
        # btnfrm1.pack(side=tk.BOTTOM)  # put them at the bottom of the form

        # then pack everything else
        self.canvas.create_window((0, 0), window=self.scrollable_frame, anchor="nw")
        self.canvas.pack(side="left", fill="both", expand=True)
        self.container.pack(fill="both", expand=True)

        self.root.mainloop()

    # def chk_command(self):
    #     # print(self.CheckVar1.get(), self.CheckVar2.get())
    #     return self.CheckVar1.get(), self.CheckVar2.get()

    def destroy(self):
        self.destroy()

    def execute_config(self):
        if self.CheckVar1.get() == 1:
            print("*****Selected only NCU data to download*****")
        elif self.CheckVar2.get() == 1:
            print("*****Selected only SPC data to download*****")
        elif self.CheckVar3.get() == 1:
            print("*****Selected only GHI data to download*****")
        elif self.CheckVar1.get() == 1 and self.CheckVar2.get() == 1:
            print("*****Selected to download both SPC and NCU data*****")

        con.chk_bool(self.CheckVar1.get(), self.CheckVar2.get(), self.CheckVar3.get())
        con.chk_resample_bool(self.Chk15min.get(), self.Chk10min.get(), self.Chk5min.get())
        import data_downloader

    def reset_config(self):
        """Rebuilds the ConfigManager from the defaultdict."""
        print('Rebuilding form from defaultdict')
        self.build(self.defaultdict)

    def apply_config(self):
        """Saves the contents of the form to configpath if one was passed."""
        # collect all the inputs
        all_inputs = []
        for child in self._fields:  # filter getting by widget class
            if isinstance(child, ttk.Entry):
                all_inputs.append(child.get())
            if isinstance(child, tk.scrolledtext.ScrolledText):
                text = child.get(1.0, 'end')
                all_inputs.append(text)

        new_parser_dict = {}
        for section in self._sections:
            new_parser_dict[section] = {}
            for section_key, input in zip(self._section_keys, all_inputs):
                if section_key in self.parser_dict[section]:
                    # configparser uses ordereddicts by default
                    # this should maintain their order
                    new_parser_dict[section][section_key] = input

        parser = configparser.ConfigParser()
        parser.read_dict(new_parser_dict)

        if self.configpath is None:
            print(f'Not saving to file because configpath is {self.configpath}')
        else:
            with open(self.configpath, 'w') as configfile:
                parser.write(configfile)

        # reset the form to reflect the changes
        self.build(new_parser_dict)

    def as_dict(self, config):

        """
        Converts a ConfigParser object into a dictionary.
        The resulting dictionary has sections as keys which point to a dict of the
        sections options as key : value pairs.
        https://stackoverflow.com/a/23944270
        """
        the_dict = {}
        for section in config.sections():
            the_dict[section] = {}
            for key, val in config.items(section):
                the_dict[section][key] = val
        return the_dict


if __name__ == '__main__':
    ConfigManager()
