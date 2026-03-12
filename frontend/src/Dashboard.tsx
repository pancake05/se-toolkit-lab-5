import { useState, useEffect } from 'react';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend,
  ChartOptions,
} from 'chart.js';
import { Bar, Line } from 'react-chartjs-2';
import './Dashboard.css';

// Register ChartJS components
ChartJS.register(
  CategoryScale,
  LinearScale,
  BarElement,
  LineElement,
  PointElement,
  Title,
  Tooltip,
  Legend
);

// Types for API responses
interface ScoreBucket {
  bucket: string;
  count: number;
}

interface TimelinePoint {
  date: string;
  submissions: number;
}

interface PassRate {
  task: string;
  avg_score: number;
  attempts: number;
}

interface DashboardProps {
  token: string;
}

type LabId = 'lab-04' | 'lab-05' | 'lab-06' | 'lab-07' | 'lab-08';

const LABS: LabId[] = ['lab-04', 'lab-05', 'lab-06', 'lab-07', 'lab-08'];

type FetchState<T> = 
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'success'; data: T }
  | { status: 'error'; message: string };

function Dashboard({ token }: DashboardProps) {
  const [selectedLab, setSelectedLab] = useState<LabId>('lab-04');
  
  const [scoresState, setScoresState] = useState<FetchState<ScoreBucket[]>>({ status: 'idle' });
  const [timelineState, setTimelineState] = useState<FetchState<TimelinePoint[]>>({ status: 'idle' });
  const [passRatesState, setPassRatesState] = useState<FetchState<PassRate[]>>({ status: 'idle' });

  useEffect(() => {
    if (!token) return;

    const fetchData = async () => {
      // Fetch scores
      setScoresState({ status: 'loading' });
      try {
        const scoresRes = await fetch(`/analytics/scores?lab=${selectedLab}`, {
          headers: { Authorization: `Bearer ${token}` },
        });
        if (!scoresRes.ok) throw new Error(`HTTP ${scoresRes.status}`);
        const scoresData = await scoresRes.json() as ScoreBucket[];
        setScoresState({ status: 'success', data: scoresData });
      } catch (err) {
        const error = err as Error;
        setScoresState({ status: 'error', message: error.message });
      }

      // Fetch timeline
      setTimelineState({ status: 'loading' });
      try {
        const timelineRes = await fetch(`/analytics/timeline?lab=${selectedLab}`, {
          headers: { Authorization: `Bearer ${token}` },
        });
        if (!timelineRes.ok) throw new Error(`HTTP ${timelineRes.status}`);
        const timelineData = await timelineRes.json() as TimelinePoint[];
        setTimelineState({ status: 'success', data: timelineData });
      } catch (err) {
        const error = err as Error;
        setTimelineState({ status: 'error', message: error.message });
      }

      // Fetch pass rates
      setPassRatesState({ status: 'loading' });
      try {
        const passRatesRes = await fetch(`/analytics/pass-rates?lab=${selectedLab}`, {
          headers: { Authorization: `Bearer ${token}` },
        });
        if (!passRatesRes.ok) throw new Error(`HTTP ${passRatesRes.status}`);
        const passRatesData = await passRatesRes.json() as PassRate[];
        setPassRatesState({ status: 'success', data: passRatesData });
      } catch (err) {
        const error = err as Error;
        setPassRatesState({ status: 'error', message: error.message });
      }
    };

    fetchData();
  }, [token, selectedLab]);

  const handleLabChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    setSelectedLab(e.target.value as LabId);
  };

  // Prepare chart data
  const barChartData = {
    labels: scoresState.status === 'success' ? scoresState.data.map(d => d.bucket) : [],
    datasets: [
      {
        label: 'Number of Students',
        data: scoresState.status === 'success' ? scoresState.data.map(d => d.count) : [],
        backgroundColor: 'rgba(75, 192, 192, 0.6)',
        borderColor: 'rgb(75, 192, 192)',
        borderWidth: 1,
      },
    ],
  };

  const lineChartData = {
    labels: timelineState.status === 'success' ? timelineState.data.map(d => d.date) : [],
    datasets: [
      {
        label: 'Submissions',
        data: timelineState.status === 'success' ? timelineState.data.map(d => d.submissions) : [],
        fill: false,
        borderColor: 'rgb(75, 192, 192)',
        backgroundColor: 'rgba(75, 192, 192, 0.5)',
        tension: 0.1,
      },
    ],
  };

  // Separate options for Bar and Line charts
  const barChartOptions: ChartOptions<'bar'> = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: 'top',
      },
    },
  };

  const lineChartOptions: ChartOptions<'line'> = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        position: 'top',
      },
    },
  };

  return (
    <div className="dashboard">
      <header className="dashboard-header">
        <h2>Analytics Dashboard</h2>
        <div className="lab-selector">
          <label htmlFor="lab-select">Select Lab:</label>
          <select
            id="lab-select"
            value={selectedLab}
            onChange={handleLabChange}
          >
            {LABS.map(lab => (
              <option key={lab} value={lab}>{lab.toUpperCase()}</option>
            ))}
          </select>
        </div>
      </header>

      <div className="charts-container">
        {/* Score Distribution Chart */}
        <div className="chart-card">
          <h3>Score Distribution</h3>
          {scoresState.status === 'loading' && <p>Loading scores...</p>}
          {scoresState.status === 'error' && <p className="error">Error: {scoresState.message}</p>}
          {scoresState.status === 'success' && (
            <div className="chart-wrapper">
              <Bar data={barChartData} options={barChartOptions} />
            </div>
          )}
        </div>

        {/* Timeline Chart */}
        <div className="chart-card">
          <h3>Submissions Timeline</h3>
          {timelineState.status === 'loading' && <p>Loading timeline...</p>}
          {timelineState.status === 'error' && <p className="error">Error: {timelineState.message}</p>}
          {timelineState.status === 'success' && (
            <div className="chart-wrapper">
              <Line data={lineChartData} options={lineChartOptions} />
            </div>
          )}
        </div>

        {/* Pass Rates Table */}
        <div className="chart-card">
          <h3>Task Pass Rates</h3>
          {passRatesState.status === 'loading' && <p>Loading pass rates...</p>}
          {passRatesState.status === 'error' && <p className="error">Error: {passRatesState.message}</p>}
          {passRatesState.status === 'success' && (
            <table className="pass-rates-table">
              <thead>
                <tr>
                  <th>Task</th>
                  <th>Average Score</th>
                  <th>Attempts</th>
                </tr>
              </thead>
              <tbody>
                {passRatesState.data.map((item, index) => (
                  <tr key={`${item.task}-${index}`}>
                    <td>{item.task}</td>
                    <td>{item.avg_score.toFixed(1)}%</td>
                    <td>{item.attempts}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      </div>
    </div>
  );
}

export default Dashboard;