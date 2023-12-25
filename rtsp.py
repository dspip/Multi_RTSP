#!/usr/bin/env python
# -*- coding:utf-8 vi:ts=4:noexpandtab
# Simple RTSP server. Run as-is or with a command-line to replace the default pipeline

from utils.gst import GLib, Gst, GstRtspServer
from utils.logger import logger
import utils.gstapp as gstapp

loop = GLib.MainLoop()


class MkvH264(gstapp.AppsinkSender):
    def __init__(self, uri, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.uri = uri
        self.container_name = "d"
        self.pipeline: "Gst.Pipeline" = None
        self.appsink_send_hook = gstapp.AppsinkSendHook(self)
        self.eos_restart_hook = gstapp.EosRestartHook(self)

    def start(self, *args, **kwargs) -> "any":
        # return super().start(*args, **kwargs)
        self.stop()
        pipe = ""
        pipe += f"filesrc location=\"{self.uri}\" ! "
        pipe += "matroskademux name=matroskademux "
        pipe += "matroskademux. ! h264parse ! "
        pipe += "appsink name=appsink emit-signals=true sync=true"
        self.pipeline = gstapp.parse_launch(pipe)
        appsink: "Gst.Element" = self.pipeline.get_by_name(f"appsink")
        # appsink_receivers["start"](appsink)
        self.appsink_send_hook.start(appsink)
        # super().start(appsink)
        self.eos_restart_hook.start(appsink.get_static_pad("sink"))
        self.pipeline.set_state(Gst.State.PLAYING)

    def stop(self, *args, **kwargs) -> "any":
        # self.appsink_sender.stop()
        self.appsink_send_hook.stop()
        # super().stop()
        self.eos_restart_hook.stop()
        if self.pipeline != None:
            self.pipeline.set_state(Gst.State.NULL)
            self.pipeline = None


def make_factory(sender: "MkvH264"):
    class _Factory(GstRtspServer.RTSPMediaFactory):
        def __init__(self) -> None:
            GstRtspServer.RTSPMediaFactory.__init__(self)

        def do_create_element(self, url):
            pipe = ""
            pipe += "appsrc name=appsrc ! h264parse ! rtph264pay name=pay0 pt=96"
            pipeline = gstapp.parse_launch(pipe)
            appsrc = gstapp.AppsrcReceiver()
            appsrc.start(pipeline.get_by_name("appsrc"))
            sender.add_receiver(0, appsrc)
            # pipeline.set_state(Gst.State.PLAYING)
            return pipeline

    return _Factory()


class GstServer():
    def __init__(self, port, senders):
        self.server = GstRtspServer.RTSPServer()
        m = self.server.get_mount_points()
        for i, sender in enumerate(senders):
            f = make_factory(sender)
            f.set_shared(True)
            m.add_factory(f"/{i}", f)
            logger.info(f"\"/{i}\", {sender.uri}")
        self.server.set_service(f"{port}")
        self.server.attach(None)


index = '1'
leftDown_sender = MkvH264(f"./videos/center.mkv")
leftUp_sender = MkvH264(f"./videos/center1.mkv")
rightDown_sender = MkvH264(f"./videos/center2.mkv")
rightUp_sender = MkvH264(f"./videos/center3.mkv")


leftDown_sender.start()
leftUp_sender.start()
rightDown_sender.start()
rightUp_sender.start()

GstServer(8556, [
    leftDown_sender,
    leftUp_sender,
    rightDown_sender,
    rightUp_sender
])

###############################
# for generating 2 rtsp 
###############################

# index = 3
# left_sender_3 = MkvH264(f"./videos/{index}/left.mkv")
# center_sender_3 = MkvH264(f"./videos/{index}/center.mkv")
# right_sender_3 = MkvH264(f"./videos/{index}/right.mkv")

# left_sender_3.start()
# center_sender_3.start()
# right_sender_3.start()

# GstServer(8557, [
#     left_sender_3,
#     center_sender_3,
#     right_sender_3
# ])

loop.run()
