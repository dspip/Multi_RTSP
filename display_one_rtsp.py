from time import time
import util
from util import AppSinkWrap, AppSrcWrap, GLib, GObject, Gst, GstApp
import threading
import numpy as np

class RTSP():
    def __init__(self) -> None:
        self.uri = None
        self.pipeline = None


    def stop(self):
        if self.pipeline != None:
            self.pipeline.set_state(Gst.State.NULL)
            self.pipeline = None    

    def start(self):
        self.stop()
        pipe = ""
        pipe += "rtspsrc"
        pipe += f" location={self.uri}"
        pipe += f" latency=0"
        pipe += " ! "
        pipe += "rtph264depay ! "
        pipe += "h264parse ! "
        pipe += "avdec_h264 ! "
        pipe += "videoconvert !  "
        pipe += "autovideosink"

        self.pipeline = util.parse_launch(pipe)
        # self.pipeline.get_by_name("decode").get_static_pad("sink").add_probe(Gst.PadProbeType.BUFFER, self.osd_sink_pad_buffer_probe)
        self.pipeline.set_state(Gst.State.PLAYING)


    
if __name__ == '__main__':
    glib_loop = GLib.MainLoop()
    glib_thread = threading.Thread(target=lambda: glib_loop.run())
    glib_thread.start()
    I = RTSP()
    I.uri = "rtsp://localhost:8556/0"
    I.start()    

    glib_thread.join()
