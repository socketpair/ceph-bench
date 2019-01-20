#ifndef MYSIGNALS_H
#define MYSIGNALS_H

#include <exception>

void setup_signal_handlers();

class AbortException : public std::exception {};

void abort_if_signalled();
#endif
