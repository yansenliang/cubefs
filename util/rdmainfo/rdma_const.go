package rdmaInfo

const (
    DriverVersion 			  = "5.8-2.0.3"
    CfgKeyRdmaStatPath          = "rdmaInfoPath"

    Cmd_Dev2RDMA_MAP   = "ibdev2netdev"
    Cmd_Ethtool_Name   = "ethtool"
    Cmd_Ethtool_Para_I = "-i"
    Cmd_Ethtool_Para_S = "-S"

    Cmd_Cat_Name      = "cat"
    Bonding_File_Path = "/proc/net/bonding/"

    Cmd_Ucx_Info_Name = "ucx_info"
    Cmd_Ucx_Info_Para = "-v"

    Cmd_Ls_Pci_Name = "lspci"

    EthTool_Key_Driver  = "driver"
    EthTool_Key_Bonding = "bonding"
    EthTool_Key_Ver     = "version"
    EthTool_Key_FW_VER  = "firmware-version"

    Bonding_Key_Slave = "Slave Interface"

    Ucx_Key_Lib_Ver    = "Library version"
    Ucx_Key_Header_Ver = "API headers version"
    Ucx_Key_Git_Commit = "Git branch"
    Ucx_Key_Build_Conf = "Configured with"

    Pci_Key_Eth_Vendor = " Ethernet controller"
    Pci_Key_Mell       = "Mellanox"

	RDMA_Counter_Path = "/sys/class/infiniband/"
    RDMA_Dev_Port_Path  = "ports"
    RDMA_HW_Counter_Dir = "hw_counters"
    RDMA_SW_Counter_Dir = "counters"

    RDMA_UMP_Prefix             = "rdma"
    RDMA_HW_Counter_Prefix      = "hw"
    RDMA_SW_Counter_Prefix      = "sw"
    RDMA_Data_Counter_Prefix    = "data"
    RDMA_Stat_Default_File_Path = "/home/"

    Counter_Buffer_Max_Len  =  128
)


const (
    Data_Recv_Packet   int = iota
    Data_Recv_Data_Bytes
    Data_Send_Packet
    Data_Send_Data_Bytes
    Data_Max_Num
)

var dataInfoKey = [...]string{
    "rx_vport_rdma_unicast_packets",
    "rx_vport_rdma_unicast_bytes",
    "tx_vport_rdma_unicast_packets",
    "tx_vport_rdma_unicast_bytes",
}

var dataInfoUmpKey = [...]string{
    "recv_packets",
    "recv_bytes",
    "send_packets",
    "send_bytes",
}

const (
    UCX_SERVER_CNT  int = iota
    UCX_ACCEPT_CONN_CNT
    UCX_CLIENT_CONN_CNT
    UCX_WQE_CNT
    UCX_CQE_CNT
    UCX_TS
    UCX_MAX_NUM
)

var ucxInfoUmpKey = [...]string{
    "ucx_server_count",
    "ucx_accept_count",
    "ucx_client_count",
    "ucx_work_elem_count",
    "ucx_complete_elem_count",
    "ucx_recving_count",
    "ucx_sending_count",
    "ucx_block_time",
}

var statisticalInfoKey = [...] string {
    "rdma_input_data",
    "rdma_output_data",
    "rdma_input_pkg",
    "rdma_output_pkg",
    "rdma_input_error",
    "rdma_output_error",
    "rdma_send_drop",
    "rdma_recv_drop",
    "rdma_conn",
    "rdma_active_conn",
    "rdma_pkg_error",
    "rdma_retrans",
}
const (
    Rdma_Input_Data    int = iota
    Rdma_Output_Data
    Rdma_Input_Pkg
    Rdma_Output_Pkg
    Rdma_Input_Error
    Rdma_Output_Error
    Rdmd_Send_Drop
    Rdmd_Recv_Drop
    Rdma_Conn
    Rdma_Active_Conn
    Rdma_Pkg_Error
    Rdma_Retrans
    Rdma_Logic_MAX_NUM
)

var LogicKeys = [...][]string {
    {"rdma_recv_bytes"},
    {"rdma_send_bytes"},
    {"rdma_recv_packets"},
    {"rdma_send_packets"},

    //Rdma_Input_Error
    {"rdma_send_packets"},

    //Rdma_Output_Error
    {"rdma_send_packets"},

    //Rdmd_Send_Drop
    {"rdma_send_packets"},

    //Rdmd_Recv_Drop
    {"rdma_send_packets"},

    //Rdma_Conn
    {"rdma_send_packets"},

    //Rdma_Active_Conn
    {"rdma_send_packets"},

    //Rdma_Pkg_Error
    {"rdma_send_packets"},

    //Rdma_Retrans
    {"rdma_send_packets"},
}

