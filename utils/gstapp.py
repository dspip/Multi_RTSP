import typing
from .gst import Gst, GLib
import math
import os
import numpy as np
import json
from .timestamp import get_timestamp
from .logger import PIPELINE, logger, Logger

from events import Events

class PipelineEvents(Events):
    __events__ = ('on_eos', 'on_warning', 'on_error')

class Pipeline(Gst.Pipeline):
    _name: "str"
    logger: "Logger"
    _events: Events
    _default_on_eos: "typing.Callable[[],typing.Any]"
    _default_on_warning: "typing.Callable[[typing.Any, typing.Any],typing.Any]"
    _default_on_error: "typing.Callable[[typing.Any, typing.Any],typing.Any]"
    on_eos: "typing.Callable[[],typing.Any]"
    on_warning: "typing.Callable[[typing.Any, typing.Any],typing.Any]"
    on_error: "typing.Callable[[typing.Any, typing.Any],typing.Any]"


def pipeline_handle_bus(pipeline: "Gst.Pipeline", name=None, logger: "Logger" = logger) -> "Pipeline":
    pipeline: "Pipeline" = pipeline
    pipeline._name = name
    pipeline.logger = logger
    pipeline._events = Events(("on_eos", "on_warning", "on_error"))
    # pipeline.on_eos = lambda: pipeline.logger.info("EOS")
    # pipeline.on_warning = lambda err, debug: pipeline.logger.warning(f"{err}: {debug}")
    # pipeline.on_error = lambda err, debug: pipeline.logger.error(f"{err}: {debug}")
    pipeline._default_on_eos = lambda : pipeline.logger.info("EOS")
    pipeline._default_on_warning = lambda err, debug: pipeline.logger.warning(f"{err}: {debug}")
    pipeline._default_on_error = lambda err, debug: pipeline.logger.error(f"{err}: {debug}")
    pipeline.on_eos = pipeline._events.on_eos
    pipeline.on_warning = pipeline._events.on_warning
    pipeline.on_error = pipeline._events.on_error
    pipeline.on_eos += pipeline._default_on_eos
    pipeline.on_warning += pipeline._default_on_warning
    pipeline.on_error += pipeline._default_on_error

    bus: "Gst.Bus" = pipeline.get_bus()
    bus.add_signal_watch()

    def on_bus_message(bus, message: "Gst.Message", user_data=None):
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
                print(f"error: {err, debug}")
                pipeline.on_error(err, debug)
    bus.connect("message", on_bus_message)
    return pipeline


def parse_launch(pipeline_description, name=None, _logger=None) -> "Pipeline":
    if _logger == None:
        _logger = logger
        if name != None:
            _logger = _logger.sub(name)
    _logger.pipeline(pipeline_description, stacklevel=2)
    pipeline = Gst.parse_launch(pipeline_description)
    if pipeline.__gtype__ != Gst.Pipeline.__gtype__:
        __pipeline: "Gst.Pipeline" = Gst.Pipeline.new()
        __pipeline.add(pipeline)
        pipeline = __pipeline
    if pipeline != None:
        if name != None:
            pipeline = pipeline_handle_bus(pipeline, name, _logger)
        else:
            pipeline = pipeline_handle_bus(pipeline)
    return pipeline


class Object:
    def __init__(self) -> None:
        pass


class IStart():
    def start(self, *args, **kwargs) -> "any":
        raise Exception("\"start\" not implemented")

    def stop(self, *args, **kwargs) -> "any":
        raise Exception("\"stop\" not implemented")

    def restart(self):
        self.stop()
        self.start()


class Receiver:
    def __init__(self, *args, **kwargs) -> None:
        pass

    def on_data(self, data):
        raise Exception("\"on_data\" not implemented")


class AppsrcReceiver(Receiver, IStart):
    def __init__(self) -> None:
        self.appsrc = None
        self.need_data_handler_id = None
        self.enough_data_handler_id = None
        self.need_data = True
        self.passed = 0
        self.dropped = 0
        self.current_drop_seq = 0

    def on_need_data(self, appsrc: "Gst.Element", length, user_data=None):
        # nonlocal need_data
        # print("need_data")
        self.need_data = True

    def on_enough_data(self, appsrc, user_data=None):
        # print("enough_data, starting to drop")
        # logger.warn("enough_data, starting to drop")
        self.need_data = False

    def clean_stats(self):
        self.dropped = 0
        self.passed = 0
        self.current_drop_seq = 0

    def stop(self, *args, **kwargs) -> "any":
        if self.appsrc != None:
            if self.need_data_handler_id != None:
                self.appsrc.disconnect(self.need_data_handler_id)
                self.need_data_handler_id = None
            if self.enough_data_handler_id != None:
                self.appsrc.disconnect(self.enough_data_handler_id)
                self.enough_data_handler_id = None
            self.appsrc = None
        self.need_data = True

    def start(self, appsrc: "Gst.Element", *args, **kwargs) -> "any":
        self.clean_stats()
        self.appsrc = appsrc
        self.need_data_handler_id = self.appsrc.connect("need-data", self.on_need_data)
        self.enough_data_handler_id = self.appsrc.connect("enough-data", self.on_enough_data)

    # def modify(self, sample: "Gst.Sample"):
    #     buffer: "Gst.Buffer" = sample.get_buffer()
    #     # buffer.dts = Gst.CLOCK_TIME_NONE
    #     buffer.dts = 0
    #     return sample

    def push_sample(self, sample):
        return self.appsrc.emit("push-sample", sample)

    def push_buffer(self, buffer):
        return self.appsrc.emit("push-buffer", buffer)

    def on_data(self, sample: "Gst.Sample"):
        if self.appsrc:
            if self.need_data:
                # sample = self.modify(sample)
                ret = self.push_sample(sample)
                # logger.info(f"{ret} {sample}")
                self.passed += 1
                # if self.current_drop_seq > 0:
                #     # print(f"recovered from droping frames, dropped={self.current_drop_seq}")
                #     logger.warn(f"recovered from droping frames, dropped={self.current_drop_seq}")
                self.current_drop_seq = 0
            else:
                self.dropped += 1
                self.current_drop_seq += 1
        # if self.current_drop_seq > 0:
        #     logger.warn(f"currently dropping, dropped={self.current_drop_seq}")


# class AppsrcReceiverNoDrop(Receiver, IStart):
#     def __init__(self) -> None:
#         self.appsrc = None

#     def stop(self, *args, **kwargs) -> "any":
#         if self.appsrc != None:
#             self.appsrc = None
#         self.need_data = True

#     def start(self, appsrc: "Gst.Element", *args, **kwargs) -> "any":
#         self.appsrc = appsrc

#     def on_data(self, sample: "Gst.Sample"):
#         if self.appsrc:
#             ret = self.appsrc.emit("push-sample", sample)


class Sender:
    def __init__(self) -> None:
        self.receivers = {}
        self.receivers_lock = GLib.RecMutex()

    def add_receiver(self, id, receiver: Receiver):
        self.receivers_lock.lock()
        self.receivers[id] = receiver
        self.receivers_lock.unlock()

    def remove_receiver(self, id):
        self.receivers_lock.lock()
        self.receivers.pop(id, None)
        self.receivers_lock.unlock()

    def send_data(self, data):
        self.receivers_lock.lock()
        # for id, receiver in self.receivers.items():
        for receiver in self.receivers.values():
            try:
                receiver.on_data(data)
                # if id != "0":
                #     logger.info(id)
            except Exception as e:
                logger.exception(e)
        self.receivers_lock.unlock()


class AppsinkSendHook(IStart):
    def __init__(self, sender: Sender, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.sender = sender
        self.handler_id = None
        self.appsink = None

    def stop(self):
        if self.appsink != None:
            if self.handler_id != None:
                self.appsink.disconnect(self.handler_id)
                self.handler_id = None
                self.appsink = None
                return True
            self.appsink = None
        return False

    def start(self, appsink, *args, **kwargs):
        self.stop()
        self.appsink = appsink
        if self.appsink != None:
            self.handler_id = self.appsink.connect("new-sample", self.__on_new_sample)

    def __on_new_sample(self, appsink):
        # nonlocal first_sample
        if appsink == None:
            return Gst.FlowReturn.OK
        sample: "Gst.Sample" = appsink.emit("try-pull-sample", 1 * Gst.MSECOND)
        if sample:
            # caps: "Gst.Caps" = sample.get_caps()
            # buffer: "Gst.Buffer" = sample.get_buffer()
            # info: "Gst.MapInfo" = buffer.map(Gst.MapFlags.READ)
            # logger.info(f"{appsink.get_name()} {caps} {info.data.nbytes} {info.data.tobytes()}")
            # buffer.unmap(info)
            self.sender.send_data(sample)
        sample = None
        return Gst.FlowReturn.OK

    def get_pipeline(self) -> "Gst.Pipeline | None":
        if self.appsink:
            return self.appsink.parent
        return None


class DemuxHook(IStart):
    def __init__(self, sender: Sender, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.sender = sender
        self.pad_added_handler_id = None
        self.pad_removed_handler_id = None
        self.no_more_pads_handler_id = None
        self.demux = None

    def stop(self):
        if self.demux != None:
            if self.pad_added_handler_id != None:
                self.demux.disconnect(self.pad_added_handler_id)
                self.pad_added_handler_id = None
            if self.pad_removed_handler_id != None:
                self.demux.disconnect(self.pad_removed_handler_id)
                self.pad_removed_handler_id = None
            if self.no_more_pads_handler_id != None:
                self.demux.disconnect(self.no_more_pads_handler_id)
                self.no_more_pads_handler_id = None
            self.demux = None
            return True
        return False

    def start(self, demux, *args, **kwargs):
        self.stop()
        self.demux = demux
        if self.demux != None:
            self.pad_added_handler_id = self.demux.connect("pad-added", self.__on_pad_added)
            self.pad_removed_handler_id = self.demux.connect("pad-removed", self.__on_pad_removed)
            self.no_more_pads_handler_id = self.demux.connect("no-more-pads", self.__on_no_more_pads)

    def __on_pad_added(self, demux, pad, user_data=None):
        pass

    def __on_pad_removed(self, demux, pad, user_data=None):
        pass

    def __on_no_more_pads(self, demux, user_data=None):
        pass

    def get_pipeline(self) -> "Gst.Pipeline | None":
        if self.demux:
            return self.demux.parent
        return None


class AppsinkSender(Sender, IStart):
    def __init__(self) -> None:
        super().__init__()
        self.appsink_send_hook = AppsinkSendHook(self)

    def get_pipeline(self):
        return self.appsink_send_hook.get_pipeline()

    # def feed(self, name: "str", *args, **kwargs) -> Feeder:
    #     return AppsinkFeeder(name, *args, **kwargs)

    def feed(self, name: "str", *args, **kwargs) -> "str":
        pipe = ""
        pipe += f"appsrc name={name}"
        pipe += f" max-bytes={math.floor(10e6)}"
        pipe += " block=true"
        pipe += " format=time"
        pipe += " do-timestamp=true"
        pipe += " emit-signals=true"
        return pipe

    def stop(self, *args, **kwargs) -> "any":
        self.appsink_send_hook.stop()

    def start(self, appsink, *args, **kwargs) -> "any":
        self.appsink_send_hook.start(appsink)


class Sample():
    def __init__(self, sample: "Gst.Sample") -> None:
        self.sample: "Gst.Sample" = sample
        self.mapinfo: "Gst.MapInfo" = None
        self.buffer: "Gst.Buffer" = None

    def __del__(self):
        self.unmap()

    def unmap(self):
        if self.mapinfo != None:
            if self.buffer != None:
                self.buffer.unmap(self.mapinfo)
        self.mapinfo = None
        self.buffer = None

    def map(self, flags: "Gst.MapFlags"):
        self.unmap()
        self.buffer: "Gst.Buffer" = self.sample.get_buffer()
        ret, mapinfo = self.buffer.map(flags)
        if ret:
            self.mapinfo = mapinfo
        return ret

    def read(self):
        return self.map(Gst.MapFlags.READ)

    def as_mat(self):
        caps: "Gst.Caps" = self.sample.get_caps()
        st: "Gst.Structure" = caps.get_structure(0)
        ret, width = st.get_int("width")
        ret, height = st.get_int("height")
        bbp = 3
        size = width * height * bbp
        data = np.frombuffer(self.mapinfo.data, np.uint8, size)
        data = np.reshape(data, (height, width, bbp))
        return data

    def as_string(self):
        data = str(self.mapinfo.data, encoding="utf-8")
        return data

    def as_json(self):
        data = json.loads(str(self.mapinfo.data, encoding="utf-8"))
        return data

    def get_caps(self) -> "Gst.Caps":
        return self.sample.get_caps()


class AppsinkPuller(IStart):
    def __init__(self) -> None:
        super().__init__()
        self.appsink = None
        self.timeout = 1 * Gst.MSECOND
        self.last_sample = None

    def start(self, appsink, *args, **kwargs) -> "any":
        self.stop()
        self.appsink = appsink

    def stop(self, *args, **kwargs) -> "any":
        self.appsink = None
        self.last_sample = None

    def pull(self) -> Sample:
        if self.appsink == None:
            return None
        sample: "Gst.Sample" = self.appsink.emit("try-pull-sample", self.timeout)
        if sample != None:
            sample = Sample(sample)
            self.last_sample = sample
        return sample


class AppsinkLastSample(IStart):
    def __init__(self, name=None) -> None:
        super().__init__()
        self.name = name
        self.appsink = None
        self.handler_id = None
        self.timeout = 1 * Gst.MSECOND
        self.on_new_sample: "typing.Callable[[Sample], typing.Any]" = None
        self.last_sample = None
        self.last_pull = 0

    def stop(self):
        if self.appsink != None:
            if self.handler_id != None:
                self.appsink.disconnect(self.handler_id)
                self.handler_id = None
                self.appsink = None
                return True
            self.appsink = None
        return False

    def start(self, appsink, *args, **kwargs):
        self.stop()
        self.appsink = appsink
        if self.appsink != None:
            self.handler_id = self.appsink.connect("new-sample", self.__on_new_sample)

    def __on_new_sample(self, appsink):
        # nonlocal first_sample
        if appsink == None:
            return Gst.FlowReturn.OK
        sample: "Gst.Sample" = appsink.emit("try-pull-sample", self.timeout)
        if sample:
            # caps: "Gst.Caps" = sample.get_caps()
            # buffer: "Gst.Buffer" = sample.get_buffer()
            # info: "Gst.MapInfo" = buffer.map(Gst.MapFlags.READ)
            # logger.info(f"{appsink.get_name()} {caps} {info.data.nbytes} {info.data.tobytes()}")
            # buffer.unmap(info)
            self.last_sample = Sample(sample)
            if self.on_new_sample:
                self.on_new_sample(self.last_sample)
        else:
            self.last_sample = None
        self.last_pull += 1
        return Gst.FlowReturn.OK

    def get_pipeline(self) -> "Gst.Pipeline | None":
        if self.appsink:
            return self.appsink.parent
        return None

    def pull(self) -> Sample:
        # logger.info(f"{self.name}: last_pull={self.last_pull}")
        self.last_pull = 0
        return self.last_sample


class AppsinkSenderSimple(AppsinkSender):
    def __init__(self, appsink: "Gst.Element", feed: "str" = None, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.appsink = appsink
        self.extended_feed = feed

    def feed(self, name: "str", *args, **kwargs) -> "str":
        pipe = ""
        pipe += super().feed(name, *args, **kwargs)
        if self.extended_feed != None:
            pipe += " ! "
            pipe += self.extended_feed
        return pipe

    def start(self, *args, **kwargs) -> "any":
        return self.appsink_send_hook.start(self.appsink)

    def stop(self, *args, **kwargs) -> "any":
        return self.appsink_send_hook.stop()

    def __del__(self):
        self.stop()


class AppsinkSenderSimple(AppsinkSender):
    def __init__(self, appsink: "Gst.Element", feed: "str" = None, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.appsink = appsink
        self.extended_feed = feed

    def feed(self, name: "str", *args, **kwargs) -> "str":
        pipe = ""
        pipe += super().feed(name, *args, **kwargs)
        if self.extended_feed != None:
            pipe += " ! "
            pipe += self.extended_feed
        return pipe

    def start(self, *args, **kwargs) -> "any":
        return self.appsink_send_hook.start(self.appsink)

    def stop(self, *args, **kwargs) -> "any":
        return self.appsink_send_hook.stop()


class AppsinkSenderSuperSimple(AppsinkSenderSimple):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.start()


class EosRestartHook(IStart):
    def __init__(self, restartable: "IStart") -> None:
        self.restartable = restartable
        self.sinkpad = None
        self.probe_id = None

    def stop(self):
        if self.sinkpad != None:
            if self.probe_id != None:
                self.sinkpad.remove_probe(self.probe_id)
                self.probe_id = None
                self.sinkpad = None
                return True
            self.sinkpad = None
        return False

    def start(self, sinkpad: "Gst.Pad"):
        # sinkpad: "Gst.Pad" = appsink.get_static_pad("sink")
        self.stop()
        self.sinkpad = sinkpad
        self.probe_id = self.sinkpad.add_probe(Gst.PadProbeType.EVENT_DOWNSTREAM, self.on_eos_probe)

    def on_eos_probe(self, pad: "Gst.Pad", info: "Gst.PadProbeInfo", user_data=None):
        event = info.get_event()
        if event.type == Gst.EventType.EOS:
            logger.info("EOS ~~~~~ PROBE")
            GLib.idle_add(self.restartable.restart)
        return Gst.PadProbeReturn.OK


class NoBuffersRestartHook(IStart):
    def __init__(self, restartable: "IStart") -> None:
        self.restartable = restartable
        self.sinkpad = None
        self.probe_id = None
        # self.timeout_handle_id = None
        self.timeout = None
        self.num_buffers = 0
        self.num_buffers_last_checked = 0

    def stop(self):
        if self.timeout != None:
            def stopper():
                logger.warn("stopper")
                return False
            self.timeout.set_callback(stopper)
            # self.timeout.unref()
            self.timeout.destroy()
            self.timeout = None
        if self.sinkpad != None:
            if self.probe_id != None:
                self.sinkpad.remove_probe(self.probe_id)
                self.probe_id = None
            self.sinkpad = None
        self.num_buffers = 0
        # logger.error(self.timeout_handle_id)
        # if self.timeout_handle_id != None:
        #     GLib.Source.remove(self.timeout_handle_id)
        #     # GLib.Source.destroy(self.timeout_handle_id)
        #     self.timeout_handle_id = None

    def start(self, sinkpad: "Gst.Pad"):
        self.stop()
        self.sinkpad = sinkpad
        self.probe_id = self.sinkpad.add_probe(Gst.PadProbeType.BUFFER, self.on_buffer)
        # self.timeout_handle_id = GLib.timeout_add(2000, self.check_still_running)  # interval in milliseconds
        self.timeout = GLib.timeout_source_new(2000)
        self.timeout.set_callback(self.check_still_running)
        self.timeout.attach()

    def on_buffer(self, pad: "Gst.Pad", info: "Gst.PadProbeInfo", user_data=None):
        self.num_buffers += 1
        return Gst.PadProbeReturn.OK

    def check_still_running(self, user_data=None):
        current_num_buffers = self.num_buffers
        # logger.info(f"{current_num_buffers}")
        if self.num_buffers > 0:
            if current_num_buffers <= self.num_buffers_last_checked:
                logger.warn("no buffers, restarting")
                self.restartable.restart()
                return False
        self.num_buffers_last_checked = current_num_buffers
        return True


class FileSender(AppsinkSender):
    def __init__(self, uri, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.uri = uri
        self.container_name = "d"
        self.pipeline: "Gst.Pipeline" = None
        # self.appsink_sender = AppsinkSender()
        self.appsink_send_hook = AppsinkSendHook(self)
        self.eos_restart_hook = EosRestartHook(self)

    def get_container_plugin(self):
        if self.uri.endswith(".mp4"):
            return "qtdemux"
        if self.uri.endswith(".mkv"):
            return "matroskademux"
        return None

    def start(self, *args, **kwargs) -> "any":
        # return super().start(*args, **kwargs)
        self.stop()
        pipe = ""
        pipe += f"filesrc location=\"{self.uri}\" ! "
        pipe += "queue max-size-buffers=1 ! "
        pipe += "appsink name=appsink emit-signals=true sync=true"
        self.pipeline = parse_launch(pipe)

        appsink: "Gst.Element" = self.pipeline.get_by_name(f"appsink")
        # appsink_receivers["start"](appsink)
        self.appsink_send_hook.start(appsink)
        # super().start(appsink)
        self.eos_restart_hook.start(appsink.get_static_pad("sink"))
        self.pipeline.set_state(Gst.State.PLAYING)

    def stop(self):
        # self.appsink_sender.stop()
        self.appsink_send_hook.stop()
        # super().stop()
        self.eos_restart_hook.stop()
        if self.pipeline != None:
            self.pipeline.set_state(Gst.State.NULL)
            self.pipeline = None

    # class _Feeder(Feeder):
    #     def __init__(self, file_input: "FileInput", name: "str", *args, **kwargs) -> None:
    #         super().__init__(*args, **kwargs)
    #         self.name = name
    #         self.file_input = file_input

    #         def feeder_advance(template):
    #             index = 0

    #             def feed():
    #                 nonlocal index
    #                 pipe = ""
    #                 pipe += (f"{self.file_input.container_name}.{template}_{index}")
    #                 index += 1
    #                 return pipe
    #             return index, feed

    #         self.video_index, self.video = feeder_advance("video")
    #         self.audio_index, self.audio = feeder_advance("audio")
    #         self.meta_index, self.meta = feeder_advance("meta")

    #     def simple(self):
    #         return f"{self.container()} {self.any()}"

    #     def any(self):
    #         return f"{self.file_input.container_name}."

    #     def container(self) -> "str":
    #         pipe = ""
    #         pipe += f"appsrc name={self.name}"
    #         pipe += f" max-bytes={math.floor(10e6)}"
    #         pipe += " block=true"
    #         pipe += " format=time"
    #         pipe += " do-timestamp=true"
    #         pipe += " emit-signals=true"
    #         if self.file_input.uri.endswith(".mp4"):
    #             pipe += f" ! qtdemux name={self.file_input.container_name}"
    #         if self.file_input.uri.endswith(".mkv"):
    #             pipe += f" ! matroskademux name={self.file_input.container_name}"
    #         return pipe

    # def feed(self, name: "str") -> Feeder:
    #     # return FileInput._Feeder(self, name)
    #     return AppsinkFeeder(name)


class RtspSenderComplex(AppsinkSender):
    def __init__(self, uri, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.uri = uri
        self.pipeline: "Gst.Pipeline" = None
        # self.appsink_sender = AppsinkSender()
        self.appsink_send_hook = AppsinkSendHook(self)
        self.no_buffer_restart_hook = NoBuffersRestartHook(self)

    def on_new_pad(demux, pad, user_data=None):
        pass

    def start(self, *args, **kwargs) -> "any":
        # return super().start(*args, **kwargs)
        self.stop()
        pipe = ""
        pipe += f"rtspsrc location=\"{self.uri}\" latency=0 drop-on-latency=true ! "
        pipe += "identity name=rtspsrc_identity ! "
        pipe += "capsfilter caps=\"application/x-rtp, media=video\" ! "
        pipe += "appsink name=appsink emit-signals=true sync=true"
        self.pipeline = parse_launch(pipe)

        appsink: "Gst.Element" = self.pipeline.get_by_name(f"appsink")
        rtsp_identity: "Gst.Element" = self.pipeline.get_by_name(f"rtspsrc_identity")
        # appsink_receivers["start"](appsink)
        # self.appsink_sender.start(appsink)
        self.appsink_send_hook.start(appsink)
        # super().start(appsink)
        self.no_buffer_restart_hook.start(rtsp_identity.get_static_pad("src"))
        self.pipeline.set_state(Gst.State.PLAYING)

    def stop(self):
        self.no_buffer_restart_hook.stop()
        # self.appsink_sender.stop()
        self.appsink_send_hook.stop()
        # super().stop()
        if self.pipeline != None:
            self.pipeline.set_state(Gst.State.NULL)
            self.pipeline = None


class RtspSender(AppsinkSender):
    def __init__(self, uri, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.uri = uri
        self.pipeline: "Gst.Pipeline" = None
        # self.appsink_sender = AppsinkSender()
        self.appsink_send_hook = AppsinkSendHook(self)
        self.no_buffer_restart_hook = NoBuffersRestartHook(self)

    def start(self, *args, **kwargs) -> "any":
        # return super().start(*args, **kwargs)
        self.stop()
        pipe = ""
        pipe += f"rtspsrc location=\"{self.uri}\" latency=0 drop-on-latency=true ! "
        pipe += "identity name=rtspsrc_identity ! "
        pipe += "capsfilter caps=\"application/x-rtp, media=video\" ! "
        pipe += "appsink name=appsink emit-signals=true sync=true"
        self.pipeline = parse_launch(pipe)

        appsink: "Gst.Element" = self.pipeline.get_by_name(f"appsink")
        rtsp_identity: "Gst.Element" = self.pipeline.get_by_name(f"rtspsrc_identity")
        # appsink_receivers["start"](appsink)
        # self.appsink_sender.start(appsink)
        self.appsink_send_hook.start(appsink)
        # super().start(appsink)
        self.no_buffer_restart_hook.start(rtsp_identity.get_static_pad("src"))
        self.pipeline.set_state(Gst.State.PLAYING)

    def stop(self):
        self.no_buffer_restart_hook.stop()
        # self.appsink_sender.stop()
        self.appsink_send_hook.stop()
        # super().stop()
        if self.pipeline != None:
            self.pipeline.set_state(Gst.State.NULL)
            self.pipeline = None


class UdpSender(AppsinkSender):
    def __init__(self, uri, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.uri = uri
        self.pipeline: "Gst.Pipeline" = None
        # self.appsink_sender = AppsinkSender()
        self.appsink_send_hook = AppsinkSendHook(self)
        self.no_buffer_restart_hook = NoBuffersRestartHook(self)

    def start(self, *args, **kwargs) -> "any":
        # return super().start(*args, **kwargs)
        self.stop()
        pipe = ""
        pipe += f"udpsrc uri=\"{self.uri}\" ! "
        pipe += "identity name=udpsrc_identity ! "
        pipe += "appsink name=appsink emit-signals=true sync=true"
        self.pipeline = parse_launch(pipe)

        appsink: "Gst.Element" = self.pipeline.get_by_name(f"appsink")
        udpsrc_identity: "Gst.Element" = self.pipeline.get_by_name(f"udpsrc_identity")
        # appsink_receivers["start"](appsink)
        # self.appsink_sender.start(appsink)
        self.appsink_send_hook.start(appsink)
        # super().start(appsink)
        self.no_buffer_restart_hook.start(udpsrc_identity.get_static_pad("src"))
        self.pipeline.set_state(Gst.State.PLAYING)

    def stop(self):
        self.no_buffer_restart_hook.stop()
        # self.appsink_sender.stop()
        self.appsink_send_hook.stop()
        # super().stop()
        if self.pipeline != None:
            self.pipeline.set_state(Gst.State.NULL)
            self.pipeline = None

    def add_receiver(self, id, receiver: Receiver):
        return super().add_receiver(id, receiver)

    # def feed(self, name: "str") -> Feeder:
    #     return AppsinkFeeder(name)


class WebRTCReceiver():
    def __init__(self) -> None:
        pass


class WebRTCSender():
    def __init__(self) -> None:
        self.medias: "list[AppsinkSender]" = []
        self.datachannels: "list[AppsinkSender]" = []
        self.viewers = []

    def add_media(self, appsink):
        pass

    def add_datachannel(self, appsink):
        pass

    def clear(self):
        pass


def generate_caps(mat: "np.ndarray", framerate) -> "Gst.Caps":
    h, w, d = mat.shape
    _format = "BGR"
    return Gst.caps_from_string(f"video/x-raw, width={w}, height={h}, format={_format}, framerate={framerate}/1")


def mat_to_buffer(mat: "np.ndarray", dts=0, pts=0) -> "Gst.Buffer":
    buffer = Gst.Buffer.new_wrapped(mat.tobytes())
    buffer.dts = dts
    buffer.pts = pts
    return buffer


def mat_to_sample(mat: "np.ndarray", caps) -> "Gst.Sample":
    return Gst.Sample.new(mat_to_buffer(mat), caps)


class MatSender(IStart):
    def __init__(self, framerate: "int" = 0) -> None:
        super().__init__()
        self.caps = None
        self.framerate = framerate
        self.appsrc = AppsrcReceiver()

    @property
    def framerate(self):
        return self._framerate

    @framerate.setter
    def framerate(self, framerate):
        self._framerate = framerate
        self.caps = None

    def start(self, appsrc, *args, **kwargs) -> "any":
        self.stop()
        self.appsrc.start(appsrc)

    def stop(self, *args, **kwargs) -> "any":
        self.appsrc.stop()
        self.caps = None

    def push(self, mat: "np.ndarray"):
        if self.caps == None:
            self.caps = generate_caps(mat, self.framerate)
        sample = mat_to_sample(mat, self.caps)
        self.appsrc.on_data(sample)


class DisplayMat(IStart):
    def __init__(self, framerate, *args, **kwargs) -> None:
        self.appsrc = MatSender(framerate)
        # self.appsrc_receiver = AppsrcReceiverNoDrop()
        self.pipeline: "Gst.Pipeline" = None
        self.push = self.appsrc.push

    @property
    def framerate(self):
        return self.appsrc.framerate

    @framerate.setter
    def framerate(self, framerate):
        self.appsrc.framerate = framerate

    def start(self, *args, **kwargs) -> "any":
        self.stop()
        pipe = ""
        pipe += "appsrc name=appsrc is-live=true ! "
        pipe += "videoconvert ! "
        # pipe += "identity dump=true ! "
        # pipe += "nveglglessink sync=false"
        pipe += "autovideosink sync=false"
        # pipe += "fakesink dump=true"
        # pipe += "gtksink sync=false"
        self.pipeline = parse_launch(pipe)
        self.appsrc.start(self.pipeline.get_by_name("appsrc"))
        self.pipeline.set_state(Gst.State.PLAYING)

    def stop(self, *args, **kwargs) -> "any":
        self.appsrc.stop()
        if self.pipeline != None:
            self.pipeline.set_state(Gst.State.NULL)
            self.pipeline = None


class DisplayReceiver(IStart):
    def __init__(self, *args, **kwargs) -> None:
        self.appsink_sender = None
        self.appsrc_receiver = AppsrcReceiver()
        # self.appsrc_receiver = AppsrcReceiverNoDrop()
        self.pipeline: "Gst.Pipeline" = None

    def start(self, appsink_sender: "AppsinkSender", *args, **kwargs) -> "any":
        self.stop()
        self.appsink_sender = appsink_sender
        pipe = ""
        pipe += self.appsink_sender.feed("appsrc") + " ! "
        pipe += "autovideosink sync=false"
        # pipe += "autovideosink"
        self.pipeline = parse_launch(pipe)
        appsrc = self.pipeline.get_by_name("appsrc")
        self.appsink_sender.add_receiver(0, self.appsrc_receiver)
        self.appsrc_receiver.start(appsrc)
        self.pipeline.set_state(Gst.State.PLAYING)

    def stop(self, *args, **kwargs) -> "any":
        if self.appsink_sender != None:
            self.appsink_sender.remove_receiver(0)
            self.appsink_sender = None
        self.appsrc_receiver.stop()
        if self.pipeline != None:
            self.pipeline.set_state(Gst.State.NULL)
            self.pipeline = None


def create_format_location_example(send_data):
    def format_location(fragment_id):
        send_data(fragment_id)
        return f"./{fragment_id}"
    return format_location


class SplitmuxsinkFilenameInput(Sender):
    def __init__(self, create_format_location=create_format_location_example) -> None:
        super().__init__()
        self.create_format_location = create_format_location
        self.format_location = None
        self.splitmuxsink = None
        self.handle_id = None

    def stop(self, *args, **kwargs) -> "any":
        if self.splitmuxsink != None:
            if self.handle_id != None:
                self.splitmuxsink.disconnect(self.handle_id)
                self.handle_id = None
            self.splitmuxsink = None

    def start(self, splitmuxsink: "Gst.Element", *args, **kwargs) -> "any":
        self.splitmuxsink = splitmuxsink
        self.format_location = self.create_format_location(self.send_data)
        self.handle_id = splitmuxsink.connect("format-location", self.format_location_callback)

    def format_location_callback(self, splitmuxsink: "Gst.Element", fragment_id):
        filename = self.format_location(fragment_id)
        # self.change_file(filename, fragment_id)
        return filename
        # return self.select_filename(fragment_id)


class SplitmuxsinkFilenameCleanInput(SplitmuxsinkFilenameInput):
    def __init__(self, create_format_location=create_format_location_example, max_files=None) -> None:
        super().__init__(create_format_location=create_format_location)
        self.filenames = []
        self.max_files = max_files

    def start(self, splitmuxsink: "Gst.Element", *args, **kwargs) -> "any":
        self.splitmuxsink = splitmuxsink
        # max_files = kwargs.get("max_files", None)
        # if max_files == None:
        #     max_files = splitmuxsink.get_property("max-files")
        self.format_location = self.create_format_location(self.sender.send_data)
        self.handle_id = splitmuxsink.connect("format-location", self.format_location_callback)

    def format_location_callback(self, splitmuxsink: "Gst.Element", fragment_id):
        filename = super().format_location_callback(splitmuxsink, fragment_id)
        self.filenames.append(filename)
        _max_files = self.max_files if self.max_files != None else len(self.filenames)
        to_delete = len(self.filenames) - _max_files
        if to_delete > 0:
            for i in range(to_delete):
                os.unlink(self.filenames.pop(0))
        return filename


def create_select_filename(storage_path, name, container):
    def select_filename(fragment_id, timestamp=None):
        if timestamp:
            return f"{storage_path}/{name}_{timestamp}.{container}"
        return f"{storage_path}/{name}_{fragment_id}.{container}"
    return select_filename


def create_multifilesinkplus_input(multifilesinkplus, storage_path, name, container="mkv", max_files=None):
    def create_format_location(send_data):
        select_filename = create_select_filename(storage_path, name, container)

        def format_location(fragment_id):
            timestamp = get_timestamp()
            filename = select_filename(fragment_id, timestamp)
            send_data((max_files, fragment_id, timestamp))
            return filename
        return format_location
    sfi = SplitmuxsinkFilenameCleanInput(create_format_location, max_files=max_files) if max_files != None else SplitmuxsinkFilenameInput(create_format_location)
    sfi.start(multifilesinkplus)
    return sfi


def create_splitmuxsink_input(splitmuxsink, storage_path, name, container="mkv", max_files=None):
    if max_files == None:
        max_files = splitmuxsink.get_property("max-files")

    def create_format_location(emit):
        select_filename = create_select_filename(storage_path, name, container)

        def format_location(fragment_id):
            timestamp = get_timestamp()
            filename = select_filename(fragment_id, timestamp)
            emit((max_files, fragment_id, timestamp))
            return filename
        return format_location
    sfi = SplitmuxsinkFilenameCleanInput(create_format_location, max_files=max_files) if max_files != None else SplitmuxsinkFilenameInput(create_format_location)
    sfi.start(splitmuxsink)
    return sfi
