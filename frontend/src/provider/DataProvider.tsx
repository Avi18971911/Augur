import {createContext, ReactNode, useContext, useState} from "react";
import {HandlerLogAndSpanDTO} from "../backend_api";
import {LogOrSpan} from "../model/LogOrSpan.ts";
import {mapLogDtoToLog, mapSpanDTOToSpan} from "../services/LogOrSpanService.ts";

type DataContextType = {
    data: LogOrSpan[];
    setData: (data: HandlerLogAndSpanDTO[]) => void;
};

const DataContext = createContext<DataContextType | undefined>(undefined);

export const useDataContext = () => {
    const context = useContext(DataContext);
    if (!context) {
        throw new Error('useDataContext must be used within a DataProvider');
    }
    return context;
};

export const DataProvider = ({ children }: { children: ReactNode }) => {
    const [data, setDataState] = useState<LogOrSpan[]>([]);

    const setData = (rawData: Array<HandlerLogAndSpanDTO>) => {
        const data = rawData.map((item) => {
            if (item.spanDto) {
                return mapSpanDTOToSpan(item.spanDto);
            } else if (item.logDto) {
                return mapLogDtoToLog(item.logDto);
            } else {
                throw new Error('Invalid data type');
            }
        });
        setDataState(data);
    }
    return (
        <DataContext.Provider value={{ data, setData }}>
            {children}
        </DataContext.Provider>
    );
};