import numpy as np
import pandas as pd

from collections import defaultdict

from cns_analytics.backtest import History


class StateMachine:
    def __init__(self, config):
        self.history = History()
        self.state = None
        self.state_type = None
        self.config = config
        
    def change_state(self, state_type, *args, **kwargs):
        state_cls = self.config.transitions[(self.state_type, state_type)]
        self.state_type = state_type
        
        if self.state is not None:
            self.state.on_exit(self, *args, **kwargs)
        self.state = state_cls
        self.state.on_enter(self, *args, **kwargs)
    
    def on_step(self, *args, **kwargs):
        self.state.on_step(self, *args, **kwargs)


class State:
    def on_enter(self, *args, **kwargs):
        pass
    
    def on_step(self, *args, **kwargs):
        pass
    
    def on_exit(self, *args, **kwargs):
        pass