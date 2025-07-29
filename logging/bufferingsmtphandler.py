#!/usr/bin/env python
#
# Copyright 2001-2002 by Vinay Sajip. All Rights Reserved.
#
# Permission to use, copy, modify, and distribute this software and its
# documentation for any purpose and without fee is hereby granted,
# provided that the above copyright notice appear in all copies and that
# both that copyright notice and this permission notice appear in
# supporting documentation, and that the name of Vinay Sajip
# not be used in advertising or publicity pertaining to distribution
# of the software without specific, written prior permission.
# VINAY SAJIP DISCLAIMS ALL WARRANTIES WITH REGARD TO THIS SOFTWARE, INCLUDING
# ALL IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL
# VINAY SAJIP BE LIABLE FOR ANY SPECIAL, INDIRECT OR CONSEQUENTIAL DAMAGES OR
# ANY DAMAGES WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER
# IN AN ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT
# OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
#
# This file is part of the Python logging distribution. See
# http://www.red-dove.com/python_logging.html
#
"""Test harness for the logging module. Tests BufferingSMTPHandler, an alternative implementation
of SMTPHandler.
Copyright (C) 2001-2002 Vinay Sajip. All Rights Reserved.
"""
import logging, logging.handlers
import traceback


class BufferingSMTPHandler(logging.handlers.BufferingHandler):
    def __init__(self, mailhost, fromaddr, toaddrs, subject, user_and_password, capacity, port=25, use_tls=False):
        logging.handlers.BufferingHandler.__init__(self, capacity)
        #logging.Handler.__init__(self)

        self.mailhost = mailhost
        self.mailport = port
        self.use_tls = use_tls
        self.fromaddr = fromaddr
        self.toaddrs = toaddrs
        self.subject = subject
        self.user = user_and_password[0]
        self.password = user_and_password[1]
        self.setFormatter(logging.Formatter("%(asctime)s %(levelname)-5s %(message)s"))

    def emit(self, record):
        """
        Emit a record.

        Append the record. If shouldFlush() tells us to, call flush() to process
        the buffer.
        """
        if record.levelno >= self.level:
            self.buffer.append(record)
            if self.shouldFlush(record):
                self.flush()

    def flush(self):
        if len(self.buffer) > 0:
            try:
                import smtplib
                port = self.mailport
                if not port:
                    port = smtplib.SMTP_PORT
                if not self.use_tls:
                    smtp = smtplib.SMTP(self.mailhost, port)
                else:
                    smtp = smtplib.SMTP_SSL(self.mailhost, port)
                    smtp.ehlo()

                smtp.login(self.user, self.password)
                #msg = "From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n" % (self.fromaddr, string.join(self.toaddrs, ","), self.subject)
                msg = "From: %s\r\nTo: %s\r\nSubject: %s\r\n\r\n" % (self.fromaddr, ",".join(self.toaddrs), self.subject)
                for record in self.buffer:
                    s = self.format(record)
                    print(s)
                    msg = msg + s + "\r\n"

                smtp.sendmail(self.fromaddr, self.toaddrs, msg)
                smtp.quit()
            except Exception as e:
                traceback.print_exc()
                #self.handleError(None)  # no particular record
            self.buffer = []

