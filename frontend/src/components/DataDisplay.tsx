import {useDataContext} from "../provider/DataProvider.tsx";
import SpanCard from "./SpanCard.tsx";
import LogCard from "./LogCard.tsx";
import {Log, Span} from "../provider/DataProvider.tsx";



function DataDisplay() {
    const { data } = useDataContext()

    const isLog = (datum: Log | Span): datum is Log => {
        return (datum as Log).severity !== undefined;
    };

    return (
        <div>
            <div style={gridStyle}>
                {data.map((datum) => (
                    (isLog(datum))? <LogCard key={datum.id} log={datum} /> : <SpanCard key={datum.spanId} span={datum} />
                ))}
            </div>
        </div>
    );
}

const gridStyle: React.CSSProperties = {
    display: 'grid',
    gridTemplateColumns: 'repeat(auto-fill, minmax(300px, 1fr))',
    gap: '16px',
    marginLeft: '50px'
};

export default DataDisplay