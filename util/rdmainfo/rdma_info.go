package rdmaInfo

import (
	"fmt"
	"github.com/cubefs/cubefs/util/exporter"
	"io"
	"net"
	"os"
	"os/exec"
	"path"
	"strconv"
	"strings"
	"time"
)

type RDMACounterErrorCb func(interface{}, string)

type RDMAMonitorElem struct {
	CounterIndex	    int
	ErrValue       		uint64		//最大值，每秒超过此值，立即报错
	ThresholdValue	    uint64		//门限值
	AlarmCnt            uint32		//连续超过门限值
	occurCnt            uint32		//当前出现次数
}

type RDMAConfInfo struct {
	IP              string
	IsBond          bool
	DevName         string
	RdmaDevName     string
	RdmaPort        string
	SlaveName       []string
	Vendor          []string
	BondVer         string
	DriverVer       string
	FWVer           string
	UcxLibVer       string
	UcxHeaderVer    string
	UcxCommit       string
	UcxBuild        string
}

type RDMAStatisticsInfo struct {
	Counters        []uint64
	DeltaCounters   []uint64
	Monitors        []RDMAMonitorElem
	CounterUMPKey   []string
	StatInfoPath    string
}

type RDMASysInfo struct {
	IsInit          bool
	IsCounterEnable bool
	Conf            RDMAConfInfo
	StaticInfo      RDMAStatisticsInfo

	counterErrCb    RDMACounterErrorCb
	dataErrCb       RDMACounterErrorCb
	cbContext       interface{}

	//internal param
	DataCounterOff int
	UcxCounterOff  int
	counterBuffer  []byte
	counterFd      []*os.File
	dataCounterV   []uint64
	statFd         *os.File

	logicIndex      [][]int

	stopC chan struct{}
}

func defErrCb(context interface{}, errDes string) {
	errorF("error occur:%s", errDes)
}

func (info *RDMASysInfo) getNetDevName() error {
	nets, err := net.Interfaces()
	if err != nil {
		return fmt.Errorf("can not get sys interfaces info:%v", err.Error())
	}

	foundNet := false
	for _, iter := range nets {
		addrIp, err := iter.Addrs()
		if err != nil {
			continue
		}

		for _, eachIP := range addrIp {
			if strings.Contains(eachIP.String(), info.Conf.IP) {
				foundNet = true
				break
			}
		}
		if foundNet {
			info.Conf.DevName = iter.Name
			return nil
		}
	}

	return fmt.Errorf("can not found ip[%s] dev info", info.Conf.IP)
}

func (info *RDMASysInfo) getRdmaDevName() (retErr error) {
	cmd := exec.Command(Cmd_Dev2RDMA_MAP)
	result, err := cmd.Output()
	if err != nil {
		retErr = fmt.Errorf("get rdma dev failed:%s", err.Error())
		return
	}

	resultStr := string(result)
	devsInfo := strings.Split(resultStr, "\n")
	for _, dev := range devsInfo {
		if strings.Contains(dev, info.Conf.DevName) {
			values := strings.Split(dev, " ")
			if len(values) < 2 {
				retErr = fmt.Errorf("parse dev output failed, out:%s", dev)
				return
			}
			info.Conf.RdmaDevName = values[0]
			info.Conf.RdmaPort = values[2]
			return nil
		}
	}

	retErr = fmt.Errorf("no rdma dev for net dev[%s]", info.Conf.DevName)
	return
}

func (info *RDMASysInfo) getDevDriverInfo() error {
	devName := info.Conf.DevName
	if info.Conf.IsBond {
		devName = info.Conf.SlaveName[0]
	}
	cmd := exec.Command(Cmd_Ethtool_Name, Cmd_Ethtool_Para_I, devName)
	results, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("get net dev[%s] info failed:%s", cmd.String(), err.Error())
	}

	resultStr := string(results)
	outStrArr := strings.Split(resultStr, "\n")
	isBondingDev := false
	for _, line := range outStrArr {
		if strings.HasPrefix(line, EthTool_Key_Driver) {
			if strings.Contains(line, EthTool_Key_Bonding) {
				info.Conf.IsBond = true
				isBondingDev = true
			}
			continue
		}

		if strings.HasPrefix(line, EthTool_Key_Ver) {
			if isBondingDev {
				info.Conf.BondVer = strings.TrimSpace(strings.Split(line, ":")[1])
				continue
			}
			info.Conf.DriverVer = strings.TrimSpace(strings.Split(line, ":")[1])
			continue
		}

		if strings.HasPrefix(line, EthTool_Key_FW_VER) && !isBondingDev {
			info.Conf.FWVer = strings.TrimSpace(strings.Split(line, ":")[1])
			continue
		}
	}

	return nil
}

func (info *RDMASysInfo) getSlaveDevName() error {
	if !info.Conf.IsBond {
		return nil
	}
	cmd := exec.Command(Cmd_Cat_Name, Bonding_File_Path+info.Conf.DevName)
	results, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("get bond[%s] info failed:%s", info.Conf.DevName, err.Error())
	}

	resultStr := string(results)
	outStrArr := strings.Split(resultStr, "\n")
	for _, line := range outStrArr {
		if strings.Contains(line, Bonding_Key_Slave) {
			info.Conf.SlaveName = append(info.Conf.SlaveName, strings.TrimSpace(strings.Split(line, ":")[1]))
		}
	}

	return nil
}

func (info *RDMASysInfo) initNetSysInfo() (err error) {
    if err = info.getNetDevName(); err != nil {
        return
    }

    if err = info.getRdmaDevName(); err != nil {
        return
    }

	if err = info.getDevDriverInfo(); err != nil {
		return err
	}

	if info.Conf.IsBond {
		if err = info.getSlaveDevName(); err != nil {
			return err
		}

		if err = info.getDevDriverInfo(); err != nil {
			return err
		}
	}

    if err = info.getVendorInfo(); err != nil {
        return
    }

    if !info.Conf.IsBond {
        info.Conf.SlaveName = append(info.Conf.SlaveName, info.Conf.DevName)
    }

	return nil
}

func (info *RDMASysInfo) getUcxInfo() error {
	cmd := exec.Command(Cmd_Ucx_Info_Name, Cmd_Ucx_Info_Para)
	results, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("get ucx info failed:%s", err.Error())
	}

	resultStr := string(results)
	outStrArr := strings.Split(resultStr, "\n")
	for _, line := range outStrArr {
		if strings.Contains(line, Ucx_Key_Lib_Ver) {
			info.Conf.UcxLibVer = strings.TrimSpace(strings.Split(line, ":")[1])
			continue
		}

		if strings.Contains(line, Ucx_Key_Header_Ver) {
			info.Conf.UcxHeaderVer = strings.TrimSpace(strings.Split(line, ":")[1])
			continue
		}

		if strings.Contains(line, Ucx_Key_Build_Conf) {
			info.Conf.UcxBuild = strings.TrimSpace(strings.Split(line, ":")[1])
			continue
		}

		if strings.Contains(line, Ucx_Key_Git_Commit) {
			info.Conf.UcxCommit = line[strings.LastIndex(line, " ")+1:]
			continue
		}
	}

	return nil
}

func (info *RDMASysInfo) getVendorInfo() error {
	cmd := exec.Command(Cmd_Ls_Pci_Name)
	results, err := cmd.Output()
	if err != nil {
		return fmt.Errorf("get vendor info failed:%s", err.Error())
	}

	resultStr := string(results)
	outStrArr := strings.Split(resultStr, "\n")
	for _, line := range outStrArr {
		if strings.Contains(line, Pci_Key_Eth_Vendor) {
			if strings.Contains(line, Pci_Key_Mell) {
				info.Conf.Vendor = append(info.Conf.Vendor, strings.TrimSpace(line[strings.LastIndex(line, ":")+1:]))
			}
		}
	}
	return nil
}

func (info *RDMASysInfo)getUint64FromFile(file *os.File) (value uint64, err error) {
	//info.counterBuffer
	var readByte int
	readByte, err = file.ReadAt(info.counterBuffer, 0)
	if err != nil && err != io.EOF {
		return 0, err
	}

	_, err = fmt.Sscanf(string(info.counterBuffer[:readByte]), "%d", &value)
	return
}

func (info *RDMASysInfo)getDataValue() {
	for i := 0; i < len(info.dataCounterV); i++ {
		info.dataCounterV[i] = 0
	}

	for _, netDevName := range info.Conf.SlaveName {
		cmd := exec.Command(Cmd_Ethtool_Name, Cmd_Ethtool_Para_S, netDevName)
		outStr, err := cmd.Output()
		if err != nil {
			return
		}
		outStrArr := strings.Split(string(outStr), "\n")
		valueCnt := 0
		for _, line := range outStrArr {
			if valueCnt >= Data_Max_Num {
				break
			}
			for dataIndex := 0; dataIndex < Data_Max_Num; dataIndex++ {
				if strings.Contains(line, dataInfoKey[dataIndex]) {
					value := uint64(0)
					_, _ = fmt.Sscanf(strings.TrimSpace(strings.Split(line, ":")[1]), "%d", &value)
					info.dataCounterV[dataIndex] += value
					valueCnt++
				}
			}
		}
	}
	return
}

func (info *RDMASysInfo) initCounter(dirPath string, CounterType string) (err error) {
	var Files []os.DirEntry
	if Files, err = os.ReadDir(dirPath); err != nil {
		return
	}
	for _, fileInfo := range Files {
		info.StaticInfo.CounterUMPKey = append(info.StaticInfo.CounterUMPKey, fmt.Sprintf("%s_%s_%s",
							RDMA_UMP_Prefix, CounterType, fileInfo.Name()))

		var counterFd *os.File
		var counterV  uint64
		if counterFd, err = os.OpenFile(path.Join(dirPath, fileInfo.Name()), os.O_RDONLY, 0655); err != nil {
			return
		}
		info.counterFd = append(info.counterFd, counterFd)
		counterV, _    = info.getUint64FromFile(counterFd)
		info.StaticInfo.Counters      = append(info.StaticInfo.Counters, counterV)
		info.StaticInfo.DeltaCounters = append(info.StaticInfo.DeltaCounters, 0)
	}
	return nil
}

func (info *RDMASysInfo) initDataCounter() (err error) {
	info.getDataValue()
	for i, dataKey := range dataInfoUmpKey {
		info.StaticInfo.CounterUMPKey = append(info.StaticInfo.CounterUMPKey, fmt.Sprintf("%s_%s", RDMA_UMP_Prefix, dataKey))
		info.dataCounterV = append(info.dataCounterV, 0)
		info.StaticInfo.Counters = append(info.StaticInfo.Counters, info.dataCounterV[i])
		info.StaticInfo.DeltaCounters = append(info.StaticInfo.DeltaCounters, 0)
	}

	return
}

//func (info *RDMASysInfo) initUcxCounter() (err error) {
//	info.UCXMonitors = ucxnet_go.CollectUCXMonitors()
//	for index, ucxKey := range ucxInfoUmpKey {
//		info.StaticInfo.CounterUMPKey = append(info.StaticInfo.CounterUMPKey, fmt.Sprintf("%s_%s",
//			RDMA_UMP_Prefix, ucxKey))
//		info.StaticInfo.Counters = append(info.StaticInfo.Counters, info.UCXMonitors.TotalMonitors[index])
//		info.StaticInfo.DeltaCounters = append(info.StaticInfo.DeltaCounters, 0)
//	}
//	return nil
//}

func (info *RDMASysInfo) GetCounterIndexByName(key string) int {
	for keyIndex := 0 ; keyIndex < len(info.StaticInfo.CounterUMPKey); keyIndex++ {
		if strings.HasSuffix(info.StaticInfo.CounterUMPKey[keyIndex], key) {
			return keyIndex
		}
	}
	return -1
}

func (info *RDMASysInfo) initLogicArray() {
	for index := 0; index < Rdma_Logic_MAX_NUM; index++ {
		info.logicIndex = append(info.logicIndex, make([]int, 0))
		for keyIndex := 0; keyIndex < len(LogicKeys[index]); keyIndex++ {
			counterIndex := info.GetCounterIndexByName(LogicKeys[index][keyIndex])
			info.logicIndex[index] = append(info.logicIndex[index], counterIndex)
		}
	}
	return
}

func (info *RDMASysInfo) initResource() (err error){
	RDMAPath := path.Join(RDMA_Counter_Path, info.Conf.RdmaDevName, RDMA_Dev_Port_Path, info.Conf.RdmaPort)
	//init hw counter key array
	if err = info.initCounter(path.Join(RDMAPath, RDMA_HW_Counter_Dir), RDMA_HW_Counter_Prefix); err != nil {
		return
	}

	//init sw counter key array
	if err = info.initCounter(path.Join(RDMAPath, RDMA_SW_Counter_Dir), RDMA_SW_Counter_Prefix); err != nil {
		return
	}

	info.DataCounterOff = len(info.StaticInfo.Counters)
	if err = info.initDataCounter(); err != nil {
		return
	}

	//info.UcxCounterOff = len(info.StaticInfo.Counters)
	//if err = info.initUcxCounter(); err != nil {
	//	return
	//}
	info.initLogicArray()
	info.counterBuffer         = make([]byte, Counter_Buffer_Max_Len)
	info.StaticInfo.Monitors   = make([]RDMAMonitorElem, len(info.StaticInfo.Counters))
	return nil
}

func (info *RDMASysInfo) checkMonitor(value uint64, counterName string, monitor *RDMAMonitorElem, cbFunc RDMACounterErrorCb) {
	//max
	if monitor.ErrValue != 0 && value > monitor.ErrValue {
		//counter error
		cbFunc(info.cbContext, fmt.Sprintf("%s reach err value:%d, alarm value:%d", counterName, value, monitor.ErrValue))
	}
	if monitor.ThresholdValue != 0 && value > monitor.ThresholdValue {
		monitor.occurCnt += 1
		if monitor.occurCnt > monitor.AlarmCnt {
			//counter error
			cbFunc(info.cbContext, fmt.Sprintf("%s is %d times higher than threshold value:(%d), alarm cnt:%d ",
				counterName, monitor.occurCnt, monitor.ThresholdValue, monitor.AlarmCnt))
		}
	} else {
		monitor.occurCnt  = 0
	}
	return
}

func (info *RDMASysInfo) updateCounter() {
	for i := 0; i < info.DataCounterOff; i++ {
		tbObj := exporter.NewModuleTP(info.StaticInfo.CounterUMPKey[i])
		value, err := info.getUint64FromFile(info.counterFd[i])
		if err != nil {
			continue
		}
		info.StaticInfo.DeltaCounters[i] = value - info.StaticInfo.Counters[i]
		tbObj.SetWithValue(int64(info.StaticInfo.DeltaCounters[i]), nil)
		info.StaticInfo.Counters[i] = value
		info.checkMonitor(info.StaticInfo.DeltaCounters[i], info.StaticInfo.CounterUMPKey[i], &info.StaticInfo.Monitors[i], info.counterErrCb)
	}
}

func (info *RDMASysInfo) updateDataInfo() {
	info.getDataValue()
    for i := 0; i < Data_Max_Num; i++ {
    	counterIndex := info.DataCounterOff + i
		tbObj := exporter.NewModuleTP(info.StaticInfo.CounterUMPKey[counterIndex])
        info.StaticInfo.DeltaCounters[counterIndex] = info.dataCounterV[i] - info.StaticInfo.Counters[counterIndex]
        info.StaticInfo.Counters[counterIndex] = info.dataCounterV[i]
		tbObj.SetWithValue(int64(info.StaticInfo.DeltaCounters[counterIndex] >> 20), nil)
		info.checkMonitor(info.StaticInfo.DeltaCounters[counterIndex], info.StaticInfo.CounterUMPKey[counterIndex],
							&info.StaticInfo.Monitors[counterIndex], info.dataErrCb)
    }
    return
}

//func (info *RDMASysInfo) updateUcxInfo() {
//	info.UCXMonitors = ucxnet_go.CollectUCXMonitors()
//	for i := 0; i < len(ucxInfoUmpKey); i++ {
//		counterIndex := info.UcxCounterOff + i
//		tbObj := exporter.NewTPCnt(info.StaticInfo.CounterUMPKey[counterIndex])
//		info.StaticInfo.DeltaCounters[counterIndex] = info.UCXMonitors.TotalMonitors[i] - info.StaticInfo.Counters[counterIndex]
//		info.StaticInfo.Counters[counterIndex] = info.UCXMonitors.TotalMonitors[i]
//		tbObj.SetWithValue(info.StaticInfo.Counters[counterIndex], nil)
//		info.checkMonitor(info.StaticInfo.Counters[counterIndex], info.StaticInfo.CounterUMPKey[counterIndex],
//			&info.StaticInfo.Monitors[counterIndex], info.dataErrCb)
//	}
//	return
//}

func (info *RDMASysInfo) collectStaticInfo() {
	interTimer := time.NewTicker(time.Second)
	if !info.IsInit {
		return
	}
	for {
		select {
		case <-info.stopC:
			interTimer.Stop()
			return
		case <-interTimer.C:
		    info.updateCounter()
		    info.updateDataInfo()
		    //info.updateUcxInfo()
		    jsonStr, err := info.GetStatisticalInfoJson()
		    if err != nil {
                continue
            }
			info.statFd.WriteAt([]byte(jsonStr), 0)
		}
	}
}

func (info *RDMASysInfo) Stop() {
    if info.IsInit {
        close(info.stopC)
    }
    for _, iter := range info.counterFd {
        if iter != nil {
            iter.Close()
        }
    }

    if info.statFd != nil {
        info.statFd.Close()
	}
}

func (info *RDMASysInfo) getStatisticalValue(i int) uint64 {
	tot := uint64(0)
	for _, key := range info.logicIndex[i] {
		tot += info.StaticInfo.Counters[key]
	}
	return tot
}

func (info *RDMASysInfo) GetStatisticalInfoJson() (string, error) {
    if !info.IsInit {
		return "", fmt.Errorf("rdma is not init")
	}
	sb := strings.Builder{}
	sb.WriteString("{\n    \"code\": 200,\n     \"data\":{\n")
	for i, _ := range statisticalInfoKey {
		sb.WriteString(fmt.Sprintf("        \"%s\":%d,\n", statisticalInfoKey[i], info.getStatisticalValue(i)))
	}

	for i, _ := range info.StaticInfo.Counters {
		sb.WriteString(fmt.Sprintf("        \"%s\":%d", info.StaticInfo.CounterUMPKey[i], info.StaticInfo.Counters[i]))
		if i == info.DataCounterOff- 1 {
			break
		}
		sb.WriteString(",\n")
	}
	sb.WriteString("\n    }, \n    \"msg\": \"\" \n}\n")
	return sb.String(), nil
}

func (info * RDMASysInfo) RestMonitorConf() {
	for i := 0; i < len(info.StaticInfo.Monitors); i++ {
		info.StaticInfo.Monitors[i].ErrValue = 0
		info.StaticInfo.Monitors[i].AlarmCnt = 0
		info.StaticInfo.Monitors[i].ThresholdValue = 0
	}
}

func (info *RDMASysInfo) UpdateMonitor(conf *RDMAMonitorElem) {
	if conf.CounterIndex >= len(info.StaticInfo.Monitors) {
		//out of range
		return
	}

	curMon := &info.StaticInfo.Monitors[conf.CounterIndex]
	curMon.AlarmCnt       = conf.AlarmCnt
	curMon.ThresholdValue = conf.ThresholdValue
	curMon.ErrValue       = conf.ErrValue
	return
}

func (info *RDMASysInfo) SetRDMAErrCb(counterCb RDMACounterErrorCb, dataCb RDMACounterErrorCb, cbContext interface{}) {
	if counterCb != nil {
		info.counterErrCb = counterCb
	}

	if dataCb != nil {
		info.dataErrCb = dataCb
	}

	info.cbContext = cbContext
}

func NewNetSysInfo(ip, path string) (info *RDMASysInfo, err error) {
	info = &RDMASysInfo {
		IsInit: false,
		stopC: make(chan struct{}),
		dataCounterV: make([]uint64, Data_Max_Num),
	}
	info.Conf.IP = ip
	if len(path) == 0 || path == "" {
		path = RDMA_Stat_Default_File_Path + strconv.Itoa(os.Getpid())
	}
	info.StaticInfo.StatInfoPath = path

	defer func() {
	    if err != nil {
	        info.Stop()
        }
    }()

	if err = info.initNetSysInfo(); err != nil {
		return
	}

	//if err = info.getUcxInfo(); err != nil {
	//	return
	//}

	if err = info.initResource(); err != nil {
		return
	}

	info.dataErrCb    = defErrCb
	info.counterErrCb = defErrCb

    info.IsCounterEnable = true
	info.IsInit = true
	go info.collectStaticInfo()
	return
}

func InitRdmaLogHandler(logHanlder RdmaLog) {
	rdmaLog = logHanlder
}
