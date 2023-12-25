import threading
# import cv2
import numpy as np
# from matplotlib import pyplot as plt


import gi
gi.require_version('Gst', '1.0')
gi.require_version('GstApp', '1.0')
from gi.repository import GLib, GObject, Gst, GstApp
Gst.init(None)


class LoopThread(threading.Thread):
    def run(self) -> None:
        try:
            GLib.MainLoop().run()
        except Exception as e:
            pass
        return super().run()


def pipeline_handle_bus(pipeline: "Gst.Pipeline"):
    pipeline.on_eos = lambda: print("EOS")
    pipeline.on_warning = lambda err, debug: print('Warning: %s: %s\n' % (err, debug))
    pipeline.on_error = lambda err, debug: print('Error: %s: %s\n' % (err, debug))
    bus = pipeline.get_bus()
    bus.add_signal_watch()

    def on_bus_message(bus, message, user_data=None):
        global loop
        t = message.type
        if t == Gst.MessageType.EOS:
            if pipeline.on_eos != None:
                pipeline.on_eos()
        elif t == Gst.MessageType.WARNING:
            err, debug = message.parse_warning()
            if pipeline.on_warning != None:
                pipeline.on_warning(err, debug)
        elif t == Gst.MessageType.ERROR:
            err, debug = message.parse_error()
            if pipeline.on_error != None:
                pipeline.on_error(err, debug)
    bus.connect("message", on_bus_message)



def parse_launch(pipeline_description) -> "Gst.Pipeline":
    print(f"[PIPELINE][{pipeline_description}]")
    pipeline = Gst.parse_launch(pipeline_description)
    if pipeline != None:
        pipeline_handle_bus(pipeline)
    return pipeline


def parse_bin_from_description(bin_description, ghost_unlinked_pads=None) -> "Gst.Bin":
    print(f"[BIN][{bin_description}]")
    bin = Gst.parse_bin_from_description(bin_description, ghost_unlinked_pads)
    return bin


class AppSrcWrap():
    def __init__(self, appsrc) -> None:
        self.appsrc = appsrc
        self.needed_data = 0
        self.appsrc.set_emit_signals(True)
        self.need_data_id = self.appsrc.connect("need-data", self._on_need_data)
        self.enough_data_id = self.appsrc.connect("enough-data", self._on_enough_data)
        self.pull = None
        self.on_need_data = None
        self.on_enough_data = None

    def _on_need_data(self, appsrc, count, user_data=None):
        self.needed_data += count
        if self.on_need_data:
            self.on_need_data(self, appsrc, count)

    def _on_enough_data(self, appsrc, user_data=None):
        self.needed_data = 0
        if self.on_enough_data:
            self.on_enough_data(self, appsrc)

    def push(self, sample):
        # print('x')
        return self.appsrc.emit("push-sample", sample)


class AppSinkWrap():
    def __init__(self, appsink) -> None:
        self.appsink = appsink
        self.appsink.set_emit_signals(True)
        # GstApp.AppSink.set_emit_signals(true)
        self.sample_count = 0
        self.handler_id = self.appsink.connect("new-sample", self._on_new_sample)
        self.on_new_sample = None
        self.blocked = False

    def block(self):
        self.blocked = True

    def unblock(self):
        self.blocked = False
        if self.on_new_sample:
            sample = self.read()
            while True:
                if sample:
                    self.on_new_sample(sample)
                    sample = self.read()
                else:
                    break

    def __del__(self):
        if self.appsink:
            if self.handler_id > 0:
                self.appsink.disconnect(self.handler_id)

    def _on_new_sample(self, appsink):
        self.sample_count += 1
        if self.on_new_sample:
            if not self.blocked:
                sample = self.read()
                if sample:
                    self.on_new_sample(sample)
        return Gst.FlowReturn.OK

    def read(self, wait=1 * Gst.MSECOND):
        sample = self.appsink.emit("try-pull-sample", wait)
        return sample

    def get_remaining(self):
        return self.sample_count


def sample_extract_image(sample: "Gst.Sample"):
    buffer: "Gst.Buffer" = sample.get_buffer()
    info: "Gst.MapInfo" = buffer.map(Gst.MapFlags.READ)
    image = np.frombuffer(info.data, dtype=np.dtype("uint8"))
    buffer.unmap(info)
    st: "Gst.Structure" = sample.get_caps().get_structure(0)
    # shape = 3, st.get_value("height"), st.get_value("width")
    shape = st.get_value("height"), st.get_value("width"), 3
    image = image.reshape(shape)
    buffer.unmap(info)
    return image


class BinWrap():
    def __init__(self, description) -> None:
        self.pipeline = parse_launch(f"appsrc name=appsrc format=time do-timestamp=true emit-signals=true ! {description} ! appsink name=appsink emit-signals=true async=false sync=false")
        self.appsrc: "GstApp.AppSrc" = self.pipeline.get_by_name("appsrc")
        self.appsink: "GstApp.AppSink" = self.pipeline.get_by_name("appsink")
        self.appsink.connect("new-sample", self._on_new_sample)
        self.on_new_sample = None
        self.pipeline.set_state(Gst.State.PLAYING)

    def push_sample(self, sample):
        self.appsrc.push_sample(sample)

    def _on_new_sample(self, appsink, user_data=None):
        sample = self.appsink.try_pull_sample(1 * Gst.MSECOND)
        if sample and self.on_new_sample:
            self.on_new_sample(sample)
        return Gst.FlowReturn.OK

    def __del__(self):
        if self.pipeline != None:
            self.pipeline.set_state(Gst.State.NULL)
            self.pipeline = None
