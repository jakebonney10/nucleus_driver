import rclpy
import rcl_interfaces
from rclpy.node import Node
from threading import Thread
import time
import socket
from rclpy._rclpy_pybind11 import RCLError
import math

from interfaces.srv import (
    ConnectTcp,
    ConnectSerial,
    Disconnect,
    Start,
    Stop,
    StartFieldCalibration,
    Command,
)
from interfaces.msg import (
    AHRS,
    INS,
    Altimeter,
    BottomTrack,
    WaterTrack,
    CurrentProfile,
    FieldCalibration,
    IMU,
    Magnetometer,
)

# Standard ROS2 message imports
from std_msgs.msg import Float32
from sensor_msgs.msg import Imu, MagneticField, FluidPressure, Temperature
from geometry_msgs.msg import PointStamped, TwistWithCovarianceStamped
from nav_msgs.msg import Odometry
from marine_acoustic_msgs.msg import Dvl
from diagnostic_msgs.msg import DiagnosticStatus
from diagnostic_updater import Updater
from nucleus_driver import NucleusDriver


class NucleusNode(Node):

    def __init__(self):
        super().__init__("nucleus_node")

        # --- Parameter Declaration ---
        self.declare_parameter('frame_id', 'nucleus_dvl')
        self.declare_parameter('world_frame_id', 'world')
        self.declare_parameter('connection_type', 'none')
        self.declare_parameter('tcp_ip', '')
        self.declare_parameter('tcp_port', 9000)
        self.declare_parameter('tcp_password', 'nortek')
        self.declare_parameter('serial_port', '')
        self.declare_parameter('auto_connect', False)
        self.declare_parameter('auto_start', False)
        # Reconnection. The driver historically connected exactly once, during
        # node init: if that failed it logged an error and ran on forever with no
        # connection, and a link that dropped later was never noticed because the
        # read loop simply kept timing out. These two drive the retry loop in
        # packet_handling(). Set either <= 0 to restore the old one-shot behaviour.
        self.declare_parameter('reconnect_interval_s', 2.0)
        self.declare_parameter('data_timeout_s', 5.0)
        # Seconds of idle before TCP keepalive probing starts. Without keepalive a
        # yanked cable leaves a half-open socket that recv() never reports, so the
        # kernel is what has to notice. <= 0 leaves the OS defaults (~2 hours).
        self.declare_parameter('tcp_keepalive_idle_s', 5)
        self.declare_parameter('sensor_configs', rcl_interfaces.msg.ParameterValue(type=rcl_interfaces.msg.ParameterType.PARAMETER_STRING_ARRAY))
        self.declare_parameter('fluid_density', 1000.0)

        # --- Get Parameters ---
        self.frame_id = self.get_parameter('frame_id').get_parameter_value().string_value
        self.world_frame_id = self.get_parameter('world_frame_id').get_parameter_value().string_value
        self._connection_type = self.get_parameter('connection_type').get_parameter_value().string_value
        self._tcp_ip = self.get_parameter('tcp_ip').get_parameter_value().string_value
        self._tcp_port = self.get_parameter('tcp_port').get_parameter_value().integer_value
        self._tcp_password = self.get_parameter('tcp_password').get_parameter_value().string_value
        self._serial_port = self.get_parameter('serial_port').get_parameter_value().string_value
        self._auto_connect = self.get_parameter('auto_connect').get_parameter_value().bool_value
        self._auto_start = self.get_parameter('auto_start').get_parameter_value().bool_value
        self._reconnect_interval_s = self.get_parameter('reconnect_interval_s').value
        self._data_timeout_s = self.get_parameter('data_timeout_s').value
        self._tcp_keepalive_idle_s = self.get_parameter('tcp_keepalive_idle_s').value
        self._sensor_configs = self.get_parameter('sensor_configs').get_parameter_value().string_array_value
        self._fluid_density = self.get_parameter('fluid_density').value

        self.nucleus_driver = NucleusDriver()

        self.connect_tcp_service = self.create_service(ConnectTcp, "nucleus_node/connect_tcp", self.connect_tcp_callback)
        self.connect_serial_service = self.create_service(ConnectSerial, "nucleus_node/connect_serial", self.connect_serial_callback)
        self.disconnect_service = self.create_service(Disconnect, "nucleus_node/disconnect", self.disconnect_callback)
        self.start_service = self.create_service(Start, "nucleus_node/start", self.start_callback)
        self.start_service = self.create_service(StartFieldCalibration, "nucleus_node/field_calibration", self.start_field_calibration_callback)
        self.stop_service = self.create_service(Stop, "nucleus_node/stop", self.stop_callback)
        self.command_service = self.create_service(Command, "nucleus_node/command", self.command_callback)

        self.ahrs_publisher = self.create_publisher(AHRS, "nucleus_node/ahrs_packets", 10)
        self.altimeter_publisher = self.create_publisher(Altimeter, "nucleus_node/altimeter_packets", 10)
        self.bottom_track_publisher = self.create_publisher(BottomTrack, "nucleus_node/bottom_track_packets", 10)
        self.water_track_publisher = self.create_publisher(WaterTrack, "nucleus_node/water_track_packets", 10)
        self.current_profile_publisher = self.create_publisher(CurrentProfile, "nucleus_node/current_profile_packets", 10)
        self.field_calibration_publisher = self.create_publisher(FieldCalibration, "nucleus_node/field_calibration_packets", 10)
        self.imu_publisher = self.create_publisher(IMU, "nucleus_node/imu_packets", 10)
        self.ins_publisher = self.create_publisher(INS, "nucleus_node/ins_packets", 10)
        self.mag_publisher = self.create_publisher(Magnetometer, "nucleus_node/magnetometer_packets", 10)
        
        # Publishers for standard ROS2 messages
        self.altimeter_common_publisher = self.create_publisher(PointStamped, "nucleus_node/altimeter_common", 10)
        self.altimeter_beam1_common_publisher = self.create_publisher(PointStamped, "nucleus_node/altimeter_beam1_common", 10)
        self.altimeter_beam2_common_publisher = self.create_publisher(PointStamped, "nucleus_node/altimeter_beam2_common", 10)
        self.altimeter_beam3_common_publisher = self.create_publisher(PointStamped, "nucleus_node/altimeter_beam3_common", 10)

        self.sound_speed_publisher = self.create_publisher(Float32, "nucleus_node/sound_speed_common", 10)
        self.pressure_publisher = self.create_publisher(FluidPressure, "nucleus_node/pressure_common", 10)
        self.depth_publisher = self.create_publisher(Odometry, "nucleus_node/depth_odometry_common", 10)

        self.temperature_publisher = self.create_publisher(Temperature, "nucleus_node/temperature_common", 10)
        self.bottom_track_velocity_publisher = self.create_publisher(TwistWithCovarianceStamped, "nucleus_node/bottom_lock_velocity_common", 10)
        self.water_track_velocity_publisher = self.create_publisher(TwistWithCovarianceStamped, "nucleus_node/water_track_velocity_common", 10)
        # marine_acoustic_msgs/Dvl: the cross-vendor DVL message, so a consumer
        # can treat this device and e.g. a Sonardyne the same way. Carries the
        # per-beam data that the generic Twist above has nowhere to put.
        self.dvl_publisher = self.create_publisher(Dvl, "nucleus_node/dvl", 10)
        self.water_track_dvl_publisher = self.create_publisher(Dvl, "nucleus_node/water_track_dvl", 10)
        self.imu_common_publisher = self.create_publisher(Imu, "nucleus_node/imu_common", 10)
        self.mag_common_publisher = self.create_publisher(MagneticField, "nucleus_node/magnetic_common", 10)

        # This timer ensures that the auto-connect logic runs after the node is fully initialized
        self.init_timer = self.create_timer(0.1, self.initialize_sensor_connection)

        self._thread_running = False
        # Monotonic stamp of the last packet off the wire, used to spot a link that
        # went away without the socket reporting an error (half-open TCP). None
        # means "no link", so the retry loop owns the next attempt.
        self._last_packet_time = None
        # --- Diagnostics state ---
        # Written from the packet thread, read from the updater's timer. Plain
        # int increments and dict item assignment are atomic under the GIL, and
        # a diagnostic that is one packet stale does not matter, so these are
        # deliberately unlocked. The callbacks copy the dict before iterating.
        self._reconnect_count = 0
        self._packet_counts = {}
        self._unknown_packet_count = 0
        self._bt_last_time = None
        self._bt_xyz_valid = None
        self._bt_distance_valid_beams = 0
        self.packet_thread = Thread(target=self.packet_handling)

        # --- Diagnostics ---
        # Updater publishes on the absolute /diagnostics topic, so this joins the
        # global topic the other rhody drivers use rather than a namespaced one.
        self._diag_updater = Updater(self)
        self._diag_updater.setHardwareID("nucleus_1000")
        self._diag_updater.add("Connection", self._diag_connection)
        self._diag_updater.add("Decoder", self._diag_decoder)
        self._diag_updater.add("Bottom Track", self._diag_bottom_track)

        self.get_logger().info("Nucleus Node initiated")

    def initialize_sensor_connection(self):
        """
        Attempts to connect and configure the sensor based on launch parameters.
        This method is called once by a timer after node initialization.

        A failure here is no longer fatal to the session: the packet handling
        thread retries on reconnect_interval_s, so the node recovers by itself
        once the device shows up.
        """
        self.init_timer.cancel()

        if self._auto_connect:
            self.get_logger().info("Auto-connect is enabled. Attempting to connect to sensor...")
            if self._attempt_connection():
                self._post_connect_setup()
            elif self._reconnect_interval_s > 0:
                self.get_logger().warn(
                    f"Initial connection failed; retrying every {self._reconnect_interval_s} s "
                    "from the packet handler.")
        else:
            self.get_logger().info("Auto-connect is disabled. Waiting for service calls.")

        # Start the packet handling thread regardless
        self.start()

    def _attempt_connection(self):
        """
        One connection attempt over the configured transport; True on success.
        Shared by initialize_sensor_connection() and the retry loop so both
        paths behave identically.
        """
        status = False
        if self._connection_type == 'tcp':
            if not self._tcp_ip:
                self.get_logger().error("Connection type is 'tcp' but 'tcp_ip' parameter is not set.")
            else:
                status = self._connect_tcp(self._tcp_ip, self._tcp_password)
        elif self._connection_type == 'serial':
            if not self._serial_port:
                self.get_logger().error("Connection type is 'serial' but 'serial_port' parameter is not set.")
            else:
                status = self._connect_serial(self._serial_port)
        elif self._connection_type != 'none':
            self.get_logger().warn(f"Invalid connection_type '{self._connection_type}'. Skipping auto-connect.")

        if status:
            self._reconnect_count += 1
            self._enable_tcp_keepalive()
            # Only start the data-timeout clock once there is a link to watch.
            self._last_packet_time = time.monotonic()
        return status

    def _tcp_socket(self):
        if self._connection_type != 'tcp':
            return None
        return getattr(self.nucleus_driver.connection, 'tcp', None)

    def _enable_tcp_keepalive(self):
        """
        Ask the kernel to probe the peer so a vanished device (cable pulled,
        power cut) turns into a socket error instead of a socket that blocks
        forever. Same reasoning as sprintnav_driver's TcpClientTransport.
        """
        sock = self._tcp_socket()
        if sock is None or self._tcp_keepalive_idle_s <= 0:
            return
        idle = int(self._tcp_keepalive_idle_s)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            # Linux-only knobs; guarded individually so a missing one is not fatal.
            for opt, val in (('TCP_KEEPIDLE', idle),
                             ('TCP_KEEPINTVL', max(1, idle // 2)),
                             ('TCP_KEEPCNT', 3)):
                if hasattr(socket, opt):
                    sock.setsockopt(socket.IPPROTO_TCP, getattr(socket, opt), val)
            self.get_logger().info(
                f"TCP keepalive enabled (idle {idle} s); a dead peer surfaces in "
                f"roughly {idle + 3 * max(1, idle // 2)} s.")
        except Exception as e:
            self.get_logger().warn(f"Could not enable TCP keepalive: {e}")

    def _socket_is_healthy(self):
        """
        Cheap, non-destructive liveness check run from the packet thread.

        Needed because the driver's own get_connection_status() is just a flag
        set at connect time: it never goes false when the peer dies. Two things
        are checked here that read_packet() cannot distinguish from a quiet
        device: a pending socket error (what keepalive eventually produces), and
        a peer that closed cleanly, where recv() returns EOF forever. MSG_PEEK
        means the probe never consumes bytes the parser needs, and this runs on
        the same thread as the reader, so it cannot race it.
        """
        sock = self._tcp_socket()
        if sock is None:
            return True
        try:
            if sock.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR) != 0:
                return False
            if sock.recv(1, socket.MSG_PEEK | socket.MSG_DONTWAIT) == b'':
                return False   # orderly shutdown from the far end
        except (BlockingIOError, socket.timeout):
            return True        # nothing pending, which is normal
        except OSError:
            return False
        return True

    def _post_connect_setup(self):
        """
        Push configuration and start measurement after a successful connect.
        Re-run on every reconnect: a device that was power-cycled has lost
        whatever was sent to it last time.
        """
        self.get_logger().info("Successfully connected to sensor.")

        # The streaming port (9002) is a read-only data stream: no login,
        # no commands. Configuration and start/stop are owned by whatever
        # client holds the command port (e.g. the Nortek software). Skip
        # any config/start so we never write to the read-only socket.
        streaming_only = self._connection_type == 'tcp' and self._tcp_port == 9002
        if streaming_only:
            self.get_logger().info(
                "Connected on streaming port 9002 (listen-only). "
                "Skipping sensor configuration and auto-start; "
                "configure and start/stop the device via the Nortek software."
            )
            if self._sensor_configs:
                self.get_logger().warn(
                    "sensor_configs is set but will be ignored in listen-only mode."
                )
            if self._auto_start:
                self.get_logger().warn(
                    "auto_start is set but will be ignored in listen-only mode."
                )

        # Send configuration commands
        if self._sensor_configs and not streaming_only:
            self.get_logger().info("Sending configuration commands...")
            all_configs_ok = True
            for cmd in self._sensor_configs:
                reply_bytes = self.nucleus_driver.send_command(command=cmd)
                try:
                    reply_str = "".join(r.decode() for r in reply_bytes).strip()
                    # A successful command usually replies with 'OK'
                    if "OK" in reply_str:
                        self.get_logger().info(f"Command '{cmd}' -> Reply: '{reply_str}'")
                    else:
                        self.get_logger().warn(f"Command '{cmd}' -> Unexpected Reply: '{reply_str}'")
                        all_configs_ok = False
                except Exception as e:
                    self.get_logger().error(f"Failed to decode or process reply for command '{cmd}': {e}")
                    all_configs_ok = False
            
            if all_configs_ok:
                self.get_logger().info("All sensor configuration commands sent successfully.")
            else:
                self.get_logger().warn("One or more sensor configuration commands may have failed. Please check logs.")


        # Auto-start measurement
        if self._auto_start and not streaming_only:
            self.get_logger().info("Auto-start is enabled. Starting measurement.")
            reply_bytes = self.nucleus_driver.start_measurement()
            try:
                reply_str = reply_bytes[0].decode().strip()
                self.get_logger().info(f"Start measurement reply: '{reply_str}'")
            except Exception as e:
                self.get_logger().error(f"Failed to decode reply from start command: {e}")

    def _is_connected(self):
        try:
            return bool(self.nucleus_driver.connection.get_connection_status())
        except Exception:
            return False

    def _handle_connection_loss(self, reason):
        """
        Drop the link so the retry loop rebuilds it from scratch. Only ever
        called from the packet thread, which is also the only reader, so there
        is no concurrent socket use to guard against.
        """
        self.get_logger().warn(f"Connection lost: {reason}. Will reconnect.")
        self._last_packet_time = None
        try:
            if self._is_connected():
                self.nucleus_driver.disconnect()
        except Exception as e:
            self.get_logger().warn(f"Error while closing the dropped connection: {e}")

    def start(self):
        self._thread_running = True
        self.packet_thread.start()
        self.get_logger().info("Nucleus Node packet handler started")

    def stop(self):
        self._thread_running = False
        if self.packet_thread.is_alive():
            self.packet_thread.join(timeout=2)

    def _connect_tcp(self, host, password):
        self.nucleus_driver.set_tcp_configuration(host=host, port=self._tcp_port)
        status = self.nucleus_driver.connect(connection_type="tcp", password=password)
        if status:
            self.get_logger().info(f"Connected through TCP with host: {host}:{self._tcp_port}")
        else:
            self.get_logger().error(f"Failed to connect with host: {host}:{self._tcp_port}")
        return status

    def _connect_serial(self, port):
        self.nucleus_driver.set_serial_configuration(port=port)
        status = self.nucleus_driver.connect(connection_type="serial")
        if status:
            self.get_logger().info(f"Connected through serial with serial port: {port}")
        else:
            self.get_logger().error(f"Failed to connect through serial port: {port}")
        return status

    def connect_tcp_callback(self, request, response):
        response.status = self._connect_tcp(request.host, request.password)
        return response

    def connect_serial_callback(self, request, response):
        response.status = self._connect_serial(request.serial_port)
        return response

    def disconnect_callback(self, request, response):

        status = self.nucleus_driver.disconnect()

        response.status = status

        if status:
            self.get_logger().info("Successfully disconnected from Nucleus")
        else:
            self.get_logger().info("Failed to disconnect from Nucleus")

        return response

    def start_callback(self, request, response):

        if not self.nucleus_driver.connection.get_connection_status():
            self.get_logger().info("Nucleus is not connected")
            response.reply = "Nucleus is not connected"

        else:
            reply = self.nucleus_driver.start_measurement()

            try:
                response.reply = reply[0].decode()
                self.get_logger().info(f"start reply: {response.reply.strip()}")
            except Exception as e:
                response.reply = f"Failed to decode response from start command: {e}"

        return response

    def start_field_calibration_callback(self, request, response):

        if not self.nucleus_driver.connection.get_connection_status():
            self.get_logger().info("Nucleus is not connected")
            response.reply = "Nucleus is not connected"

        else:
            reply = self.nucleus_driver.start_fieldcal()

            try:
                response.reply = reply[0].decode()
                self.get_logger().info(f"start field calibration reply: {response.reply.strip()}")
            except Exception as e:
                response.reply = "Failed to decode response from start field calibration command: {e}"

        return response

    def stop_callback(self, request, response):

        if not self.nucleus_driver.connection.get_connection_status():
            self.get_logger().info("Nucleus is not connected")
            response.reply = "Nucleus is not connected"

        else:
            reply = self.nucleus_driver.stop()

            try:
                response.reply = reply[0].decode()
                self.get_logger().info(f"stop reply: {response.reply.strip()}")
            except Exception as e:
                response.reply = f"Failed to decode response from start command: {e}"

        return response

    def command_callback(self, request, response):

        if not self.nucleus_driver.connection.get_connection_status():
            self.get_logger().info("Nucleus is not connected")
            response.reply = "Nucleus is not connected"

        else:
            command = request.command
            nmea = request.nmea

            reply = self.nucleus_driver.send_command(command=command, nmea=nmea)

            try:
                for entry in reply:
                    response.reply += entry.decode()

                self.get_logger().info(f"command reply: {response.reply.strip()}")
            except Exception as e:
                response.reply = f"Failed to decode response from command: {e}"

        return response

    
    # --- Diagnostics -------------------------------------------------------
    # Task names mirror the other rhody drivers (sprintnav/adnav both expose
    # Connection + Decoder), so the Foxglove diagnostics view reads the same way
    # across sensors. Bottom Track is the DVL-specific addition: it is what
    # actually says whether the Nucleus is usable for navigation.

    PACKET_NAMES = {
        0xD2: "ahrs",
        0xDC: "ins",
        0x82: "imu",
        0x87: "magnetometer",
        0xB4: "bottom_track",
        0xBE: "water_track",
        0xAA: "altimeter",
        0xC0: "current_profile",
        0x8B: "field_calibration",
    }

    @staticmethod
    def _diag_add(stat, key, value):
        """
        Add a key/value pair, stringified.

        diagnostic_updater's Python DiagnosticStatusWrapper.add() assigns the
        value straight into KeyValue.value with no conversion — its docstring is
        a stale copy of the C++ one and still talks about std::stringstream. A
        non-str therefore fails rosidl's PyUnicode_Check assertion and aborts the
        whole process, so everything goes through str() here.
        """
        stat.add(key, str(value))

    def _count_packet(self, packet):
        packet_id = packet.get("id")
        name = self.PACKET_NAMES.get(packet_id)
        if name is None:
            self._unknown_packet_count += 1
            return
        self._packet_counts[name] = self._packet_counts.get(name, 0) + 1

        if packet_id == 0xB4:
            self._bt_last_time = time.monotonic()
            self._bt_xyz_valid = (
                bool(packet.get("status.xVelocityValid")),
                bool(packet.get("status.yVelocityValid")),
                bool(packet.get("status.zVelocityValid")),
            )
            self._bt_distance_valid_beams = sum(
                1 for k in ("status.beam1DistanceValid",
                            "status.beam2DistanceValid",
                            "status.beam3DistanceValid")
                if packet.get(k))

    # Variance to report for a channel the bottom/water track packet does not
    # measure (angular rates) or whose FOM the device flagged invalid. Large
    # rather than 0, because 0 reads as "perfectly known" to a filter and
    # robot_localization will happily believe it. Not -1 either: a negative
    # variance is not a valid covariance entry and geometry_msgs documents no
    # such sentinel (marine_acoustic_msgs/Dvl does, and uses it there).
    UNKNOWN_VARIANCE = 1e6

    def _track_velocity_msg(self, packet, ros_timestamp):
        """
        Build the TwistWithCovarianceStamped shared by bottom track (0xB4) and
        water track (0xBE), which carry identical velocity/FOM fields.

        Axes are published exactly as the device reports them, per REP-145
        ("all data should be published by the driver as it is reported by the
        device. Any subsequent modifications to the data (e.g. filtering,
        transformations) should be delegated to a downstream consumer"). The
        Nucleus reports XYZ in its instrument-referenced frame - mounted
        downward-facing (N3015-033 figure 4) that is x-forward, y-starboard,
        z-down. So frame_id must name a TF frame whose declared rotation IS
        that instrument frame; the vehicle URDF owns that, not this driver.
        An earlier revision negated y and z here to force REP-103 axes, which
        contradicted REP-145 and left the IMU/magnetometer topics in a
        different convention from the velocity ones.

        Covariance is the one thing not taken verbatim: FOM is "estimated
        velocity uncertainty reported as one standard deviation", in m/s
        (N3015-033 section 8.9), and the message field is a variance, so it is
        squared. That is filling the message contract, not transforming data.
        """
        vel_msg = TwistWithCovarianceStamped()
        vel_msg.header.stamp = ros_timestamp
        vel_msg.header.frame_id = self.frame_id

        # Instrument axes, unmodified (REP-145).
        vel_msg.twist.twist.linear.x = packet["velocityX"]
        vel_msg.twist.twist.linear.y = packet["velocityY"]
        vel_msg.twist.twist.linear.z = packet["velocityZ"]

        # FOM (one sigma, m/s) -> variance. Invalid FOMs are reported by the
        # device as the sentinel 10.0, so honour the validity bits rather than
        # squaring the sentinel into a 100 m^2/s^2 variance.
        for index, fom_key, valid_key in ((0,  "fomX", "status.xFomValid"),
                                          (7,  "fomY", "status.yFomValid"),
                                          (14, "fomZ", "status.zFomValid")):
            if packet.get(valid_key):
                vel_msg.twist.covariance[index] = packet[fom_key] ** 2
            else:
                vel_msg.twist.covariance[index] = self.UNKNOWN_VARIANCE

        # The DVL measures no angular rate at all.
        for index in (21, 28, 35):
            vel_msg.twist.covariance[index] = self.UNKNOWN_VARIANCE

        return vel_msg

    # Nucleus slanted beam angle off nadir, from the Nucleus 1000 datasheet
    # (rev 03/23/2025): "Slanted beam angle 20 deg". Kept in step with
    # slant_deg in rhody's config/sensor_fov.yaml.
    BEAM_SLANT_RAD = math.radians(20.0)

    def _track_dvl_msg(self, packet, ros_timestamp, velocity_mode):
        """
        Build a marine_acoustic_msgs/Dvl from a bottom track (0xB4) or water
        track (0xBE) packet. Field meanings are from N3015-033 section 8.9.

        Axes are the instrument's own, unmodified, like every other topic
        from this node (REP-145). Dvl.msg additionally documents its own
        convention -- "z-axis in direction DVL is pointing, x-axis aligned
        with vehicle forward", i.e. FRD -- which is what the downward-mounted
        Nucleus reports natively, so the two agree without any conversion.
        """
        m = Dvl()
        m.header.stamp = ros_timestamp
        m.header.frame_id = self.frame_id
        m.velocity_mode = velocity_mode
        # Dvl.msg lists Nortek DVLs under DVL_TYPE_PISTON (one piston
        # transducer per beam), which is what the Nucleus is.
        m.dvl_type = Dvl.DVL_TYPE_PISTON

        m.velocity.x = float(packet["velocityX"])
        m.velocity.y = float(packet["velocityY"])
        m.velocity.z = float(packet["velocityZ"])

        # FOM is one standard deviation in m/s, so square it. Off-diagonals and
        # anything the device flagged invalid are -1, which Dvl.msg explicitly
        # documents as "not known" (unlike geometry_msgs, hence the different
        # sentinel from _track_velocity_msg).
        for i in range(9):
            m.velocity_covar[i] = -1.0
        for index, fom_key, valid_key in ((0, "fomX", "status.xFomValid"),
                                          (4, "fomY", "status.yFomValid"),
                                          (8, "fomZ", "status.zFomValid")):
            if packet.get(valid_key):
                m.velocity_covar[index] = float(packet[fom_key]) ** 2

        # Per-beam data. The Nucleus has three slanted beams, so slot 4 of
        # Dvl.msg's fixed-size [4] arrays is unused and left at its default.
        good_beams = 0
        vertical_sum = 0.0
        for i in range(3):
            n = i + 1
            if packet.get(f"status.beam{n}VelocityValid"):
                m.beam_velocity[i] = float(packet[f"velocityBeam{n}"])
            else:
                # Invalid velocities are reported as the sentinel -32.768 m/s.
                m.beam_velocity_covar[i] = -1.0
            if packet.get(f"status.beam{n}DistanceValid"):
                d = float(packet[f"distanceBeam{n}"])
                vertical_sum += d
                good_beams += 1
                # Section 8.9 calls this the "vertical distance to cell along
                # beam N", i.e. already projected onto nadir, whereas Dvl.msg
                # wants the slant range along the beam so that
                # beam_unit_vec*range lands on the seafloor. Undo the
                # projection. NOTE: that wording is self-contradictory
                # ("vertical ... along beam"); if a bench check shows these are
                # raw slant ranges, drop this division and the altitude below
                # needs a cos() instead.
                m.range[i] = d / math.cos(self.BEAM_SLANT_RAD)
            else:
                m.range_covar[i] = -1.0
            if packet.get(f"status.beam{n}FomValid"):
                # Dvl.msg leaves beam_quality DVL-specific; the natural value
                # here is the per-beam FOM (one sigma, m/s).
                m.beam_quality[i] = float(packet[f"fomBeam{n}"])
        # Unused fourth slot.
        m.range_covar[3] = -1.0
        m.beam_velocity_covar[3] = -1.0

        m.num_good_beams = good_beams
        # Average altitude of the good beams. Already vertical per the note
        # above, so no trigonometric factor is applied.
        m.altitude = vertical_sum / good_beams if good_beams else 0.0
        m.beam_ranges_valid = good_beams > 0
        m.beam_velocities_valid = any(
            packet.get(f"status.beam{n}VelocityValid") for n in (1, 2, 3))

        # beam_unit_vec is deliberately left zeroed: neither N3015-033 nor the
        # datasheet maps beam index to azimuth about the instrument x-axis, and
        # a guessed geometry would silently mislocate every seafloor point.
        # rhody's sensor_fov.yaml assumes 0/120/240 deg for drawing only.

        m.course_gnd = math.atan2(m.velocity.y, m.velocity.x)
        m.speed_gnd = math.hypot(m.velocity.x, m.velocity.y)
        m.sound_speed = float(packet["soundSpeed"])
        return m

    def _listen_only(self):
        return self._connection_type == 'tcp' and self._tcp_port == 9002

    def _diag_connection(self, stat):
        connected = self._is_connected()
        age = (None if self._last_packet_time is None
               else time.monotonic() - self._last_packet_time)
        # Staleness threshold for *reporting*. data_timeout_s is deliberately 0
        # in listen-only mode (silence there is legitimate), so it cannot double
        # as the reporting threshold or this task would never go stale at all.
        stale_after = self._data_timeout_s if self._data_timeout_s > 0 else 5.0

        if not connected:
            stat.summary(DiagnosticStatus.ERROR, "not connected")
        elif age is None:
            stat.summary(DiagnosticStatus.WARN, "connected, no data yet")
        elif age > stale_after:
            if self._listen_only():
                # Not an error: an external client owns start/stop, so this is
                # just how a device nobody has started measuring on looks.
                stat.summary(DiagnosticStatus.WARN,
                             "connected, no data - is measurement started?")
            else:
                stat.summary(DiagnosticStatus.ERROR, "data timeout")
        else:
            stat.summary(DiagnosticStatus.OK, "receiving")

        self._diag_add(stat, "mode", "listen-only" if self._listen_only() else "full control")
        self._diag_add(stat, "transport", self._connection_type)
        self._diag_add(stat, "endpoint", f"{self._tcp_ip}:{self._tcp_port}"
                 if self._connection_type == 'tcp' else self._serial_port)
        self._diag_add(stat, "connected", connected)
        self._diag_add(stat, "seconds since last data", "never" if age is None else round(age, 2))
        self._diag_add(stat, "connects", self._reconnect_count)
        self._diag_add(stat, "reconnect interval s", self._reconnect_interval_s)
        self._diag_add(stat, "data timeout s", self._data_timeout_s)
        self._diag_add(stat, "keepalive idle s", self._tcp_keepalive_idle_s)
        return stat

    def _diag_decoder(self, stat):
        counts = dict(self._packet_counts)
        total = sum(counts.values())
        unknown = self._unknown_packet_count

        if total == 0 and unknown == 0:
            stat.summary(DiagnosticStatus.STALE, "no packets decoded yet")
        elif unknown:
            # Usually a firmware/parser mismatch: the device is emitting packet
            # IDs this build does not know, which is otherwise silent data loss.
            stat.summary(DiagnosticStatus.WARN,
                         f"{unknown} unrecognised packet(s) - firmware/parser mismatch?")
        else:
            stat.summary(DiagnosticStatus.OK, f"decoding {total} packets")

        self._diag_add(stat, "total packets", total)
        self._diag_add(stat, "unrecognised packets", unknown)
        for name in sorted(counts):
            self._diag_add(stat, f"packets: {name}", counts[name])
        return stat

    def _diag_bottom_track(self, stat):
        if self._bt_last_time is None:
            stat.summary(DiagnosticStatus.STALE, "no bottom track packets yet")
            self._diag_add(stat, "bottom lock", False)
            return stat

        age = time.monotonic() - self._bt_last_time
        xyz = self._bt_xyz_valid or (False, False, False)
        locked = all(xyz)

        if age > 5.0:
            stat.summary(DiagnosticStatus.WARN, f"no bottom track for {age:.1f} s")
        elif locked:
            stat.summary(DiagnosticStatus.OK, "bottom lock")
        else:
            # Expected off-bottom or out of range: the vehicle simply has no DVL
            # velocity solution right now.
            stat.summary(DiagnosticStatus.WARN, "no bottom lock")

        self._diag_add(stat, "bottom lock", locked)
        self._diag_add(stat, "x/y/z velocity valid", "{}/{}/{}".format(*xyz))
        self._diag_add(stat, "beams with valid distance", self._bt_distance_valid_beams)
        self._diag_add(stat, "seconds since last bottom track", round(age, 2))
        return stat

    def packet_handling(self):

        while self._thread_running and rclpy.ok():

            # Reconnect loop. This thread is the only reader of the socket, so
            # owning (re)connection here keeps all socket access single-threaded
            # and needs no locking against the read below.
            if self._auto_connect and self._reconnect_interval_s > 0 and (
                    not self._is_connected() or not self._socket_is_healthy()):
                if self._is_connected():
                    self._handle_connection_loss("socket no longer healthy")
                time.sleep(self._reconnect_interval_s)
                if not (self._thread_running and rclpy.ok()):
                    break
                if self._attempt_connection():
                    self._post_connect_setup()
                continue

            packet = self.nucleus_driver.read_packet(timeout=1, _suppress_warning=True)

            if packet is None:
                # A read timeout is normal when the device is simply quiet, but a
                # half-open TCP link looks exactly the same from here: the socket
                # never errors and read_packet just keeps returning None. Treat a
                # long enough silence as a dead link so the loop above rebuilds it.
                if (self._data_timeout_s > 0 and self._last_packet_time is not None
                        and time.monotonic() - self._last_packet_time > self._data_timeout_s):
                    self._handle_connection_loss(
                        f"no data for {self._data_timeout_s} s")
                continue

            self._last_packet_time = time.monotonic()
            self._count_packet(packet)

            system_timestamp = self.get_clock().now()
            ros_timestamp = system_timestamp.to_msg()

            if packet["id"] == 0xD2:

                ahrs_packet = AHRS()
                ahrs_packet.system_timestamp = ros_timestamp
                ahrs_packet.posix_time = packet["flags.posixTime"]
                ahrs_packet.timestamp = packet["timeStamp"]
                ahrs_packet.microseconds = packet["microSeconds"]
                ahrs_packet.serial_number = packet["serialNumber"]
                ahrs_packet.operation_mode = packet["operationMode"]
                ahrs_packet.fom_ahrs = packet["fomAhrs"]
                ahrs_packet.fom_fc1 = packet["fomFc1"]
                ahrs_packet.roll = packet["ahrsData.roll"]
                ahrs_packet.pitch = packet["ahrsData.pitch"]
                ahrs_packet.heading = packet["ahrsData.heading"]
                ahrs_packet.quaternion_w = packet["ahrsData.quaternionW"]
                ahrs_packet.quaternion_x = packet["ahrsData.quaternionX"]
                ahrs_packet.quaternion_y = packet["ahrsData.quaternionY"]
                ahrs_packet.quaternion_z = packet["ahrsData.quaternionZ"]
                ahrs_packet.rotation_matrix_0 = packet["ahrsData.rotationMatrix_0"]
                ahrs_packet.rotation_matrix_1 = packet["ahrsData.rotationMatrix_1"]
                ahrs_packet.rotation_matrix_2 = packet["ahrsData.rotationMatrix_2"]
                ahrs_packet.rotation_matrix_3 = packet["ahrsData.rotationMatrix_3"]
                ahrs_packet.rotation_matrix_4 = packet["ahrsData.rotationMatrix_4"]
                ahrs_packet.rotation_matrix_5 = packet["ahrsData.rotationMatrix_5"]
                ahrs_packet.rotation_matrix_6 = packet["ahrsData.rotationMatrix_6"]
                ahrs_packet.rotation_matrix_7 = packet["ahrsData.rotationMatrix_7"]
                ahrs_packet.rotation_matrix_8 = packet["ahrsData.rotationMatrix_8"]
                ahrs_packet.declination = packet["declination"]
                ahrs_packet.depth = packet["depth"]

                try:
                    self.ahrs_publisher.publish(ahrs_packet)

                except RCLError:
                    pass
                except Exception as e:
                    self.get_logger().error(f"Failed to publish AHRS or Imu packet: {e}")

            elif packet["id"] == 0xDC:

                ins_packet = INS()
                ins_packet.system_timestamp = ros_timestamp
                ins_packet.posix_time = packet["flags.posixTime"]
                ins_packet.timestamp = packet["timeStamp"]
                ins_packet.microseconds = packet["microSeconds"]
                ins_packet.serial_number = packet["serialNumber"]
                ins_packet.operation_mode = packet["operationMode"]
                ins_packet.fom_ahrs = packet["fomAhrs"]
                ins_packet.fom_fc1 = packet["fomFc1"]
                ins_packet.roll = packet["ahrsData.roll"]
                ins_packet.pitch = packet["ahrsData.pitch"]
                ins_packet.heading = packet["ahrsData.heading"]
                ins_packet.quaternion_w = packet["ahrsData.quaternionW"]
                ins_packet.quaternion_x = packet["ahrsData.quaternionX"]
                ins_packet.quaternion_y = packet["ahrsData.quaternionY"]
                ins_packet.quaternion_z = packet["ahrsData.quaternionZ"]
                ins_packet.rotation_matrix_0 = packet["ahrsData.rotationMatrix_0"]
                ins_packet.rotation_matrix_1 = packet["ahrsData.rotationMatrix_1"]
                ins_packet.rotation_matrix_2 = packet["ahrsData.rotationMatrix_2"]
                ins_packet.rotation_matrix_3 = packet["ahrsData.rotationMatrix_3"]
                ins_packet.rotation_matrix_4 = packet["ahrsData.rotationMatrix_4"]
                ins_packet.rotation_matrix_5 = packet["ahrsData.rotationMatrix_5"]
                ins_packet.rotation_matrix_6 = packet["ahrsData.rotationMatrix_6"]
                ins_packet.rotation_matrix_7 = packet["ahrsData.rotationMatrix_7"]
                ins_packet.rotation_matrix_8 = packet["ahrsData.rotationMatrix_8"]
                ins_packet.declination = packet["declination"]
                ins_packet.depth = packet["depth"]
                ins_packet.fom_ins = packet["fomIns"]
                ins_packet.lat_long_is_valid = packet["statusIns.latLonIsValid"]
                ins_packet.course_over_ground = packet["courseOverGround"]
                ins_packet.temperature = packet["temperature"]
                ins_packet.pressure = packet["pressure"]
                ins_packet.altitude = packet["altitude"]
                ins_packet.latitude = packet["latitude"]
                ins_packet.longitude = packet["longitude"]
                ins_packet.position_frame_x = packet["positionFrameX"]
                ins_packet.position_frame_y = packet["positionFrameY"]
                ins_packet.position_frame_z = packet["positionFrameZ"]
                ins_packet.velocity_ned_x = packet["velocityNedX"]
                ins_packet.velocity_ned_y = packet["velocityNedY"]
                ins_packet.velocity_ned_z = packet["velocityNedZ"]
                ins_packet.velocity_nucleus_x = packet["velocityNucleusX"]
                ins_packet.velocity_nucleus_y = packet["velocityNucleusY"]
                ins_packet.velocity_nucleus_z = packet["velocityNucleusZ"]
                ins_packet.speed_over_ground = packet["speedOverGround"]
                ins_packet.turn_rate_x = packet["turnRateX"]
                ins_packet.turn_rate_y = packet["turnRateY"]
                ins_packet.turn_rate_z = packet["turnRateZ"]

                try:
                    self.ins_publisher.publish(ins_packet)
                except RCLError:
                    pass
                except Exception as e:
                    self.get_logger().error(f"Failed to publish INS-derived packets: {e}")

            elif packet["id"] == 0x82:

                imu_packet = IMU()
                imu_packet.system_timestamp = ros_timestamp
                imu_packet.posix_time = packet["flags.posixTime"]
                imu_packet.timestamp = packet["timeStamp"]
                imu_packet.microseconds = packet["microSeconds"]
                imu_packet.is_valid = packet["status.isValid"]
                imu_packet.has_data_path_overrun = packet["status.hasDataPathOverrun"]
                imu_packet.has_flash_update_failure = packet["status.hasFlashUpdateFailure"]
                imu_packet.has_spi_com_error = packet["status.hasSpiComError"]
                imu_packet.has_low_voltage = packet["status.hasLowVoltage"]
                imu_packet.has_sensor_failure = packet["status.hasSensorFailure"]
                imu_packet.has_memory_failure = packet["status.hasMemoryFailure"]
                imu_packet.has_gyro_1_failure = packet["status.hasGyro1Failure"]
                imu_packet.has_gyro_2_failure = packet["status.hasGyro2Failure"]
                imu_packet.has_accelerometer_failure = packet["status.hasAccelerometerFailure"]
                imu_packet.accelerometer_x = packet["accelerometer.x"]
                imu_packet.accelerometer_y = packet["accelerometer.y"]
                imu_packet.accelerometer_z = packet["accelerometer.z"]
                imu_packet.gyro_x = packet["gyro.x"]
                imu_packet.gyro_y = packet["gyro.y"]
                imu_packet.gyro_z = packet["gyro.z"]
                imu_packet.temperature = packet["temperature"]

                try:
                    self.imu_publisher.publish(imu_packet)

                    # Publish standard sensor_msgs/Imu with accel and gyro
                    imu_msg = Imu()
                    imu_msg.header.stamp = ros_timestamp
                    imu_msg.header.frame_id = self.frame_id
                    imu_msg.linear_acceleration.x = packet["accelerometer.x"]
                    imu_msg.linear_acceleration.y = packet["accelerometer.y"]
                    imu_msg.linear_acceleration.z = packet["accelerometer.z"]
                    imu_msg.angular_velocity.x = packet["gyro.x"]
                    imu_msg.angular_velocity.y = packet["gyro.y"]
                    imu_msg.angular_velocity.z = packet["gyro.z"]
                    
                    # Indicate no orientation
                    imu_msg.orientation_covariance[0] = -1.0
                    
                    self.imu_common_publisher.publish(imu_msg)

                except RCLError:
                    pass
                except Exception as e:
                    self.get_logger().error(f"Failed to publish IMU packet: {e}")

            elif packet["id"] == 0x87:

                mag_packet = Magnetometer()
                mag_packet.system_timestamp = ros_timestamp
                mag_packet.posix_time = packet["flags.posixTime"]
                mag_packet.timestamp = packet["timeStamp"]
                mag_packet.microseconds = packet["microSeconds"]
                mag_packet.is_compensated_for_hard_iron = packet["status.isCompensatedForHardIron"]
                mag_packet.dvl_active = packet["status.dvlActive"]
                mag_packet.dvl_acoustics_active = packet["status.dvlAcousticsActive"]
                mag_packet.dvl_transmitter_active = packet["status.dvlTransmitterActive"]
                mag_packet.magnetometer_x = packet["magnetometer.x"]
                mag_packet.magnetometer_y = packet["magnetometer.y"]
                mag_packet.magnetometer_z = packet["magnetometer.z"]

                try:
                    self.mag_publisher.publish(mag_packet)

                    # Publish standard sensor_msgs/MagneticField
                    mag_msg = MagneticField()
                    mag_msg.header.stamp = ros_timestamp
                    mag_msg.header.frame_id = self.frame_id
                    mag_msg.magnetic_field.x = packet["magnetometer.x"]
                    mag_msg.magnetic_field.y = packet["magnetometer.y"]
                    mag_msg.magnetic_field.z = packet["magnetometer.z"]
                    self.mag_common_publisher.publish(mag_msg)

                except RCLError:
                    pass
                except Exception as e:
                    self.get_logger().error(f"Failed to publish Magnetometer packet: {e}")

            elif packet["id"] == 0xB4:

                bottom_track_packet = BottomTrack()
                bottom_track_packet.system_timestamp = ros_timestamp
                bottom_track_packet.posix_time = packet["flags.posixTime"]
                bottom_track_packet.timestamp = packet["timeStamp"]
                bottom_track_packet.microseconds = packet["microSeconds"]
                bottom_track_packet.beam_1_velocity_valid = packet["status.beam1VelocityValid"]
                bottom_track_packet.beam_2_velocity_valid = packet["status.beam2VelocityValid"]
                bottom_track_packet.beam_3_velocity_valid = packet["status.beam3VelocityValid"]
                bottom_track_packet.beam_1_distance_valid = packet["status.beam1DistanceValid"]
                bottom_track_packet.beam_2_distance_valid = packet["status.beam2DistanceValid"]
                bottom_track_packet.beam_3_distance_valid = packet["status.beam3DistanceValid"]
                bottom_track_packet.beam_1_fom_valid = packet["status.beam1FomValid"]
                bottom_track_packet.beam_2_fom_valid = packet["status.beam2FomValid"]
                bottom_track_packet.beam_3_fom_valid = packet["status.beam3FomValid"]
                bottom_track_packet.x_velocity_valid = packet["status.xVelocityValid"]
                bottom_track_packet.y_velocity_valid = packet["status.yVelocityValid"]
                bottom_track_packet.z_velocity_valid = packet["status.zVelocityValid"]
                bottom_track_packet.x_fom_valid = packet["status.xFomValid"]
                bottom_track_packet.y_fom_valid = packet["status.yFomValid"]
                bottom_track_packet.z_fom_valid = packet["status.zFomValid"]
                bottom_track_packet.serial_number = packet["serialNumber"]
                bottom_track_packet.sound_speed = packet["soundSpeed"]
                bottom_track_packet.temperature = packet["temperature"]
                bottom_track_packet.pressure = packet["pressure"]
                bottom_track_packet.velocity_beam_1 = packet["velocityBeam1"]
                bottom_track_packet.velocity_beam_2 = packet["velocityBeam2"]
                bottom_track_packet.velocity_beam_3 = packet["velocityBeam3"]
                bottom_track_packet.distance_beam_1 = packet["distanceBeam1"]
                bottom_track_packet.distance_beam_2 = packet["distanceBeam2"]
                bottom_track_packet.distance_beam_3 = packet["distanceBeam3"]
                bottom_track_packet.fom_beam_1 = packet["fomBeam1"]
                bottom_track_packet.fom_beam_2 = packet["fomBeam2"]
                bottom_track_packet.fom_beam_3 = packet["fomBeam3"]
                bottom_track_packet.dt_beam_1 = packet["dtBeam1"]
                bottom_track_packet.dt_beam_2 = packet["dtBeam2"]
                bottom_track_packet.dt_beam_3 = packet["dtBeam3"]
                bottom_track_packet.time_vel_beam_1 = packet["timeVelBeam1"]
                bottom_track_packet.time_vel_beam_2 = packet["timeVelBeam2"]
                bottom_track_packet.time_vel_beam_3 = packet["timeVelBeam3"]
                bottom_track_packet.velocity_x = packet["velocityX"]
                bottom_track_packet.velocity_y = packet["velocityY"]
                bottom_track_packet.velocity_z = packet["velocityZ"]
                bottom_track_packet.fom_x = packet["fomX"]
                bottom_track_packet.fom_y = packet["fomY"]
                bottom_track_packet.fom_z = packet["fomZ"]
                bottom_track_packet.dt_xyz = packet["dtXYZ"]

                try:
                    self.bottom_track_publisher.publish(bottom_track_packet)

                    if packet["status.xVelocityValid"] and packet["status.yVelocityValid"] and packet["status.zVelocityValid"]:
                        self.bottom_track_velocity_publisher.publish(
                            self._track_velocity_msg(packet, ros_timestamp))
                        self.dvl_publisher.publish(
                            self._track_dvl_msg(packet, ros_timestamp,
                                                Dvl.DVL_MODE_BOTTOM))
                    
                    if packet["status.beam1DistanceValid"]:
                        alt_b1_msg = PointStamped()
                        alt_b1_msg.header.stamp = ros_timestamp
                        alt_b1_msg.header.frame_id = self.frame_id
                        alt_b1_msg.point.x = 0.0
                        alt_b1_msg.point.y = 0.0
                        alt_b1_msg.point.z = packet["distanceBeam1"]
                        self.altimeter_beam1_common_publisher.publish(alt_b1_msg)

                    if packet["status.beam2DistanceValid"]:
                        alt_b2_msg = PointStamped()
                        alt_b2_msg.header.stamp = ros_timestamp
                        alt_b2_msg.header.frame_id = self.frame_id
                        alt_b2_msg.point.x = 0.0
                        alt_b2_msg.point.y = 0.0
                        alt_b2_msg.point.z = packet["distanceBeam2"]
                        self.altimeter_beam2_common_publisher.publish(alt_b2_msg)

                    if packet["status.beam3DistanceValid"]:
                        alt_b3_msg = PointStamped()
                        alt_b3_msg.header.stamp = ros_timestamp
                        alt_b3_msg.header.frame_id = self.frame_id
                        alt_b3_msg.point.x = 0.0
                        alt_b3_msg.point.y = 0.0
                        alt_b3_msg.point.z = packet["distanceBeam3"]
                        self.altimeter_beam3_common_publisher.publish(alt_b3_msg)

                except RCLError:
                    pass
                except Exception as e:
                    self.get_logger().error(f"Failed to publish Bottom Track derived packets: {e}")

            elif packet["id"] == 0xBE:

                water_track_packet = WaterTrack()
                water_track_packet.system_timestamp = ros_timestamp
                water_track_packet.posix_time = packet["flags.posixTime"]
                water_track_packet.timestamp = packet["timeStamp"]
                water_track_packet.microseconds = packet["microSeconds"]
                water_track_packet.beam_1_velocity_valid = packet["status.beam1VelocityValid"]
                water_track_packet.beam_2_velocity_valid = packet["status.beam2VelocityValid"]
                water_track_packet.beam_3_velocity_valid = packet["status.beam3VelocityValid"]
                water_track_packet.beam_1_distance_valid = packet["status.beam1DistanceValid"]
                water_track_packet.beam_2_distance_valid = packet["status.beam2DistanceValid"]
                water_track_packet.beam_3_distance_valid = packet["status.beam3DistanceValid"]
                water_track_packet.beam_1_fom_valid = packet["status.beam1FomValid"]
                water_track_packet.beam_2_fom_valid = packet["status.beam2FomValid"]
                water_track_packet.beam_3_fom_valid = packet["status.beam3FomValid"]
                water_track_packet.x_velocity_valid = packet["status.xVelocityValid"]
                water_track_packet.y_velocity_valid = packet["status.yVelocityValid"]
                water_track_packet.z_velocity_valid = packet["status.zVelocityValid"]
                water_track_packet.x_fom_valid = packet["status.xFomValid"]
                water_track_packet.y_fom_valid = packet["status.yFomValid"]
                water_track_packet.z_fom_valid = packet["status.zFomValid"]
                water_track_packet.serial_number = packet["serialNumber"]
                water_track_packet.sound_speed = packet["soundSpeed"]
                water_track_packet.temperature = packet["temperature"]
                water_track_packet.pressure = packet["pressure"]
                water_track_packet.velocity_beam_1 = packet["velocityBeam1"]
                water_track_packet.velocity_beam_2 = packet["velocityBeam2"]
                water_track_packet.velocity_beam_3 = packet["velocityBeam3"]
                water_track_packet.distance_beam_1 = packet["distanceBeam1"]
                water_track_packet.distance_beam_2 = packet["distanceBeam2"]
                water_track_packet.distance_beam_3 = packet["distanceBeam3"]
                water_track_packet.fom_beam_1 = packet["fomBeam1"]
                water_track_packet.fom_beam_2 = packet["fomBeam2"]
                water_track_packet.fom_beam_3 = packet["fomBeam3"]
                water_track_packet.dt_beam_1 = packet["dtBeam1"]
                water_track_packet.dt_beam_2 = packet["dtBeam2"]
                water_track_packet.dt_beam_3 = packet["dtBeam3"]
                water_track_packet.time_vel_beam_1 = packet["timeVelBeam1"]
                water_track_packet.time_vel_beam_2 = packet["timeVelBeam2"]
                water_track_packet.time_vel_beam_3 = packet["timeVelBeam3"]
                water_track_packet.velocity_x = packet["velocityX"]
                water_track_packet.velocity_y = packet["velocityY"]
                water_track_packet.velocity_z = packet["velocityZ"]
                water_track_packet.fom_x = packet["fomX"]
                water_track_packet.fom_y = packet["fomY"]
                water_track_packet.fom_z = packet["fomZ"]
                water_track_packet.dt_xyz = packet["dtXYZ"]

                try:
                    self.water_track_publisher.publish(water_track_packet)

                    if packet["status.xVelocityValid"] and packet["status.yVelocityValid"] and packet["status.zVelocityValid"]:
                        self.water_track_velocity_publisher.publish(
                            self._track_velocity_msg(packet, ros_timestamp))
                        self.water_track_dvl_publisher.publish(
                            self._track_dvl_msg(packet, ros_timestamp,
                                                Dvl.DVL_MODE_WATER))

                except RCLError:
                    pass
                except Exception as e:
                    self.get_logger().error(f"Failed to publish Water Track packet: {e}")

            elif packet["id"] == 0xAA:

                altimeter_packet = Altimeter()
                altimeter_packet.system_timestamp = ros_timestamp
                altimeter_packet.posix_time = packet["flags.posixTime"]
                altimeter_packet.timestamp = packet["timeStamp"]
                altimeter_packet.microseconds = packet["microSeconds"]
                altimeter_packet.altimeter_distance_valid = packet["status.altimeterDistanceValid"]
                altimeter_packet.altimeter_quality_valid = packet["status.altimeterQualityValid"]
                altimeter_packet.pressure_valid = packet["status.pressureValid"]
                altimeter_packet.temperature_valid = packet["status.temperatureValid"]
                altimeter_packet.serial_number = packet["serialNumber"]
                altimeter_packet.sound_speed = packet["soundSpeed"]
                altimeter_packet.temperature = packet["temperature"]
                altimeter_packet.pressure = packet["pressure"]
                altimeter_packet.altimeter_distance = packet["altimeterDistance"]
                altimeter_packet.altimeter_quality = packet["altimeterQuality"]

                try:
                    self.altimeter_publisher.publish(altimeter_packet)

                    if packet["status.altimeterDistanceValid"]:
                        alt_msg = PointStamped()
                        alt_msg.header.stamp = ros_timestamp
                        alt_msg.header.frame_id = self.frame_id
                        alt_msg.point.x = 0.0
                        alt_msg.point.y = 0.0
                        alt_msg.point.z = packet["altimeterDistance"]
                        self.altimeter_common_publisher.publish(alt_msg)
                    
                    if packet["status.pressureValid"]:
                        pressure_msg = FluidPressure()
                        pressure_msg.header.stamp = ros_timestamp
                        pressure_msg.header.frame_id = self.frame_id
                        pressure_msg.fluid_pressure = packet["pressure"]
                        self.pressure_publisher.publish(pressure_msg)

                        depth_msg = Odometry()
                        depth_msg.header.stamp = ros_timestamp
                        depth_msg.header.frame_id = self.world_frame_id
                        depth_msg.child_frame_id = self.frame_id
                        # convert the pressure (Bar) to depth (m)
                        depth_converted = (packet["pressure"] * 100000.0) / (self._fluid_density * 9.81)
                        depth_msg.pose.pose.position.x = 0.0
                        depth_msg.pose.pose.position.y = 0.0
                        depth_msg.pose.pose.position.z = -depth_converted
                        self.depth_publisher.publish(depth_msg)

                    if packet["status.temperatureValid"]:
                        temp_msg = Temperature()
                        temp_msg.header.stamp = ros_timestamp
                        temp_msg.header.frame_id = self.frame_id
                        temp_msg.temperature = packet["temperature"]
                        self.temperature_publisher.publish(temp_msg)
                    
                    sound_speed_mgs = Float32()
                    sound_speed_mgs.data = packet['soundSpeed']
                    self.sound_speed_publisher.publish(sound_speed_mgs)

                except RCLError:
                    pass
                except Exception as e:
                    self.get_logger().error(f"Failed to publish Altimeter derived packets: {e}")

            elif packet["id"] == 0xC0:

                current_profile_packet = CurrentProfile()
                current_profile_packet.system_timestamp = ros_timestamp
                current_profile_packet.posix_time = packet["flags.posixTime"]
                current_profile_packet.timestamp = packet["timeStamp"]
                current_profile_packet.microseconds = packet["microSeconds"]
                current_profile_packet.serial_number = packet["serialNumber"]
                current_profile_packet.sound_velocity = packet["soundVelocity"]
                current_profile_packet.temperature = packet["temperature"]
                current_profile_packet.pressure = packet["pressure"]
                current_profile_packet.cell_size = packet["cellSize"]
                current_profile_packet.blanking = packet["blanking"]
                current_profile_packet.number_of_cells = packet["numberOfCells"]
                current_profile_packet.ambiguity_velocity = packet["ambiguityVelocity"]
                velocity_data = list()
                amplitude_data = list()
                correlation_data = list()
                for key in packet.keys():
                    if "velocityData" in key:
                        velocity_data.append(packet[key])
                    elif "amplitudeData" in key:
                        amplitude_data.append(packet[key])
                    elif "correlationData" in key:
                        correlation_data.append(packet[key])
                current_profile_packet.velocity_data = velocity_data
                current_profile_packet.amplitude_data = amplitude_data
                current_profile_packet.correlation_data = correlation_data

                try:
                    self.current_profile_publisher.publish(current_profile_packet)

                except RCLError:
                    pass
                except Exception as e:
                    self.get_logger().error(f"Failed to publish Current Profile packet: {e}")

            elif packet["id"] == 0x8B:

                field_calibration_packet = FieldCalibration()
                field_calibration_packet.system_timestamp = ros_timestamp
                field_calibration_packet.posix_time = packet["flags.posixTime"]
                field_calibration_packet.timestamp = packet["timeStamp"]
                field_calibration_packet.microseconds = packet["microSeconds"]
                field_calibration_packet.points_used_in_estimation = packet["status.pointsUsedInEstimation"]
                field_calibration_packet.hard_iron_x = packet["hardIron.x"]
                field_calibration_packet.hard_iron_y = packet["hardIron.y"]
                field_calibration_packet.hard_iron_z = packet["hardIron.z"]
                field_calibration_packet.s_axis_0 = packet["sAxis_0"]
                field_calibration_packet.s_axis_1 = packet["sAxis_1"]
                field_calibration_packet.s_axis_2 = packet["sAxis_2"]
                field_calibration_packet.s_axis_3 = packet["sAxis_3"]
                field_calibration_packet.s_axis_4 = packet["sAxis_4"]
                field_calibration_packet.s_axis_5 = packet["sAxis_5"]
                field_calibration_packet.s_axis_6 = packet["sAxis_6"]
                field_calibration_packet.s_axis_7 = packet["sAxis_7"]
                field_calibration_packet.s_axis_8 = packet["sAxis_8"]
                field_calibration_packet.new_point_x = packet["newPoint.x"]
                field_calibration_packet.new_point_y = packet["newPoint.y"]
                field_calibration_packet.new_point_z = packet["newPoint.z"]
                field_calibration_packet.fom_field_calibration = packet["fomFieldCalibration"]
                field_calibration_packet.coverage = packet["coverage"]

                try:
                    self.field_calibration_publisher.publish(field_calibration_packet)
                except RCLError:
                    pass
                except Exception as e:
                    self.get_logger().error(f"Failed to publish Field Calibration packet: {e}")


def main():

    rclpy.init()

    nucleus_node = NucleusNode()

    try:
        rclpy.spin(nucleus_node)

    except KeyboardInterrupt:
        print('Keyboard Interrupt (SIGINT)')

    except Exception as e:
        nucleus_node.get_logger().error(f'An unexpected error occurred: {e}')

    finally:
        nucleus_node.stop()

        if nucleus_node.nucleus_driver.connection.get_connection_status():
            stop_status = nucleus_node.nucleus_driver.stop()
            print(f'stop reply: {stop_status[0].decode().strip()}')

            disconnect_status = nucleus_node.nucleus_driver.disconnect()
            print(f'Disconnect status {disconnect_status}')

        nucleus_node.destroy_node()

        if rclpy.ok():
            rclpy.shutdown()


if __name__ == "__main__":
    main()
