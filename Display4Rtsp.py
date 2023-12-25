from time import time
import util
from util import AppSinkWrap, AppSrcWrap, GLib, GObject, Gst, GstApp
import threading
import numpy as np
from time import sleep


class RTSP():
    def __init__(self, uri, id) -> None:
        self.uri = uri
        self.pipeline = None
        self.idobj = id
        self.appsrc = None
        width, height = 960, 540 
        self.last_sample_time = time()
        self.black_screen_timeout = 0.05
        black_image = np.zeros((height, width, 3), dtype=np.uint8)
        self.bufferPTS = Gst.Buffer.new_wrapped(black_image.tobytes())
        
        caps = Gst.Caps.from_string("video/x-raw, format=(string)I420, width=(int)960, height=(int)540, interlace-mode=(string)progressive, pixel-aspect-ratio=(fraction)1/1, chroma-site=(string)mpeg2, colorimetry=(string)1:3:5:1, framerate=(fraction)30/1")
        
        self.new_sample = Gst.Sample.new(self.bufferPTS,caps,None,None) 
        




    def display_black_screen(self):
        if self.appsrc is None or self.appsrc.appsrc.clock is None:
            return
        baseTime = self.appsrc.appsrc.base_time

        clock = self.appsrc.appsrc.clock.get_time()

        self.bufferPTS.pts = clock - baseTime

        self.appsrc.push(self.new_sample)

    def check_sample_timeout(self):
        
        current_time = time()
        
        if current_time - self.last_sample_time > self.black_screen_timeout:
            # print(f"No data received from {self.uri}. Displaying black screen.")
            self.display_black_screen()
        return True


    def on_new_sample(self, sample):
        if sample:
            # caps = sample.get_caps()
            # print(caps.to_string())
            if self.appsrc:
                
                self.appsrc.push(sample)
                self.last_sample_time = time()


    def stop(self):
        if self.pipeline is not None:
            self.pipeline.set_state(Gst.State.NULL)
            self.pipeline = None
        return False

    def start(self):
        self.stop()
        pipe = ""
        pipe += f"rtspsrc location={self.uri} latency=0 ! queue max-size-buffers=10 ! "
        pipe += f"rtph264depay ! h264parse ! avdec_h264 ! videoconvert ! videoscale ! video/x-raw,width=960,height=540 ! appsink name={self.idobj} emit-signals=true "
        

        self.pipeline = util.parse_launch(pipe)
        self.appsink = AppSinkWrap(self.pipeline.get_by_name(str(self.idobj)))
        self.appsink.on_new_sample = self.on_new_sample

         # Get Appsrc element by name and assign it to self.appsrc
        self.appsrc = self.pipeline.get_by_name("appsrc")
        # print(self.appsrc)
        if self.appsrc:
            # print("***********")
        # Add a GStreamer bus watcher to check for errors
            bus = self.pipeline.get_bus()
            bus.add_signal_watch()
            bus.connect("message::error", self.on_error_message)

        # Add a periodic timer to check for sample timeouts
        
        
        GLib.timeout_add(1, self.check_sample_timeout)

        
        self.pipeline.set_state(Gst.State.PLAYING)

    def on_error_message(self, bus, message):
        error, debug_info = message.parse_error()
        print(f"Error received from {self.uri}: {error.message}")
        self.display_black_screen()


class Compositor():
    def __init__(self,rtspInst):
        self.pipline = None
        self.rtspInst = rtspInst
        self.appsrcs = None


    def stop(self):
        if self.pipline is not None:
            self.pipline.set_state(Gst.State.NULL)
            self.pipline = None
    
    def start(self, width, height):
        self.stop()
        pipeCompose = " "
        pipeCompose += " compositor name=comp sink_0::xpos=0 sink_0::ypos=0 " 
        pipeCompose += f"  sink_1::xpos={width} sink_1::ypos=0 "
        pipeCompose += f"  sink_2::xpos=0 sink_2::ypos={height} "
        pipeCompose += f"  sink_3::xpos={width} sink_3::ypos={height} ! videoconvert ! video/x-raw,width=1920,height=1080 ! autovideosink"
        pipeCompose += f" appsrc is-live=true name=src0 format=3 ! queue max-size-buffers=10 ! video/x-raw,width={width},height={height} ! timeoverlay ! comp. "
        pipeCompose += f" appsrc is-live=true name=src1 format=3 ! queue max-size-buffers=10 ! video/x-raw,width={width},height={height} ! timeoverlay ! comp. "
        pipeCompose += f" appsrc is-live=true name=src2 format=3 ! queue max-size-buffers=10 ! video/x-raw,width={width},height={height} ! timeoverlay ! comp. "
        pipeCompose += f" appsrc is-live=true name=src3 format=3 ! queue max-size-buffers=10 ! video/x-raw,width={width},height={height} ! timeoverlay ! comp. "

        self.pipeline = util.parse_launch(pipeCompose)

        self.appsrcs = [AppSrcWrap(self.pipeline.get_by_name('src0')),
                         AppSrcWrap(self.pipeline.get_by_name('src1')),
                           AppSrcWrap(self.pipeline.get_by_name('src2')),
                             AppSrcWrap(self.pipeline.get_by_name('src3'))]


        self.pipeline.set_state(Gst.State.PLAYING)
        for rtsp, appsrc in zip(self.rtspInst ,self.appsrcs):
            rtsp.appsrc = appsrc


if __name__ == '__main__':
    glib_loop = GLib.MainLoop()
    glib_thread = threading.Thread(target=lambda: glib_loop.run())
    glib_thread.start()


    uris = [
        "rtsp://localhost:8556/0",
        "rtsp://localhost:8556/1",
        "rtsp://localhost:8556/2",
        "rtsp://localhost:8556/3"
    ]

    
    rtsp_instances = [RTSP(id,uri) for uri,id in enumerate(uris)]
    for rtsp_instance in rtsp_instances:
        rtsp_instance.start()


    compositor = Compositor(rtsp_instances)
    compositor.start(960,540)
    # Wait for a few seconds and then stop one of the RTSP streams to simulate a disconnect
    # sleep(5)
    #GLib.timeout_add_seconds(5,rtsp_instances[2].stop)
    #GLib.timeout_add_seconds(7,rtsp_instances[0].stop)
    #GLib.timeout_add_seconds(8,rtsp_instances[1].stop)
    #GLib.timeout_add_seconds(9,rtsp_instances[3].stop)

    # GLib.timeout_add_seconds(,rtsp_instances[3].stop)
      # Stop the third RTSP stream

    glib_thread.join()
