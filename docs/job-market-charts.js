// Job Market Pipeline Charts
// All charts load data from dashboard_data.json

let dashboardData = null;

// ============ INITIALIZATION ============

async function initDashboard() {
    try {
        const response = await fetch('dashboard_data.json');
        dashboardData = await response.json();
        
        console.log('Dashboard data loaded:', dashboardData.metadata);
        
        // Update all dynamic text elements
        updateMetrics();
        
        // Initialize charts based on which elements exist on the page
        // Index page charts
        if (document.getElementById('jobsMapChart')) createJobsMap();
        if (document.getElementById('salaryMapChart')) createSalaryMap();
        if (document.getElementById('sourceBreakdownChart')) createSourceBreakdown();
        if (document.getElementById('salaryBySourceChart')) createSalaryBySource();
        
        // Insights page charts
        if (document.getElementById('jobsMapChartInsights')) createJobsMapInsights();
        if (document.getElementById('salaryMapChartInsights')) createSalaryMapInsights();
        if (document.getElementById('salaryBySourceDetailChart')) createSalaryBySourceDetail();
        if (document.getElementById('topStatesChart')) createTopStatesChart();
        if (document.getElementById('jobTypeChart')) createJobTypeChart();
        if (document.getElementById('jobTypeBySourceChart')) createJobTypeBySource();
        if (document.getElementById('topCompaniesChart')) createTopCompaniesChart();
        if (document.getElementById('keywordChart')) createKeywordChart();
        
        // Pipeline page charts
        if (document.getElementById('pipelineFlowChart')) createPipelineFlow();
        if (document.getElementById('dagStructureChart')) createDagStructure();
        
    } catch (error) {
        console.error('Failed to load dashboard data:', error);
    }
}

// ============ DYNAMIC METRICS ============

function updateMetrics() {
    // Helper to safely set text content
    const setText = (id, text) => {
        const el = document.getElementById(id);
        if (el) el.textContent = text;
    };
    
    // Total jobs (used in multiple places)
    setText('totalJobs', dashboardData.total_jobs.toLocaleString());
    setText('heroTotalJobs', dashboardData.total_jobs.toLocaleString());
    
    // States count
    setText('statesCount', dashboardData.metadata.states_covered);
    setText('heroStatesCount', dashboardData.metadata.states_covered);
    
    // Top state
    const stateEntries = Object.entries(dashboardData.count_by_state);
    if (stateEntries.length > 0) {
        const [topState, topCount] = stateEntries[0];
        setText('metricTopState', topState);
        setText('metricTopStateDesc', `${topCount.toLocaleString()} data science postings`);
    }
    
    // Highest salary state
    const salaryEntries = Object.entries(dashboardData.avg_salary_by_state);
    if (salaryEntries.length > 0) {
        const sorted = salaryEntries.sort((a, b) => b[1] - a[1]);
        const [highState, highSalary] = sorted[0];
        setText('metricHighestSalary', `$${Math.round(highSalary / 1000)}K`);
        setText('metricHighestSalaryState', `${highState} leads`);
    }
    
    // Federal salary (USAJobs)
    const fedSalary = dashboardData.avg_salary_by_source['USAJobs'];
    if (fedSalary) {
        setText('metricFederalSalary', `$${Math.round(fedSalary / 1000)}K`);
    }
    
    // Full-time percentage
    const typeEntries = Object.entries(dashboardData.count_by_type);
    const total = Object.values(dashboardData.count_by_type).reduce((a, b) => a + b, 0);
    const fullTime = dashboardData.count_by_type['Full-time'] || 0;
    const contract = dashboardData.count_by_type['Contract'] || 0;
    if (total > 0) {
        const ftPercent = Math.round((fullTime / total) * 100);
        const ctPercent = Math.round((contract / total) * 100);
        const otherPercent = 100 - ftPercent - ctPercent;
        setText('metricFullTimePercent', `${ftPercent}%`);
        setText('metricJobTypeBreakdown', `Contract: ${ctPercent}%, Other: ${otherPercent}%`);
    }
    
    // Salary range for hero badge
    const allSalaries = Object.values(dashboardData.avg_salary_by_state);
    if (allSalaries.length > 0) {
        const minSal = Math.min(...allSalaries);
        const maxSal = Math.max(...allSalaries);
        const rangeEl = document.getElementById('heroSalaryRange');
        if (rangeEl) {
            rangeEl.textContent = `$${Math.round(minSal / 1000)}K - $${Math.round(maxSal / 1000)}K Salary Range`;
        }
    }
}

// ============ COLOR SCHEMES ============

const sourceColors = {
    'Adzuna': '#667eea',
    'USAJobs': '#764ba2',
    'Jooble': '#f5576c',
    'Other': '#a0aec0'
};

const jobTypeColors = {
    'Full-time': '#667eea',
    'Contract': '#764ba2',
    'Part-time': '#f5576c',
    'Temporary': '#feca57',
    'Internship': '#43e97b'
};

// Blue color scale (high = dark)
const blueScale = [
    [0, '#e8eaf6'],
    [0.2, '#9fa8da'],
    [0.4, '#7986cb'],
    [0.6, '#5c6bc0'],
    [0.8, '#3f51b5'],
    [1, '#1a237e']
];

// Green color scale (high = dark)
const greenScale = [
    [0, '#e8f5e9'],
    [0.2, '#a5d6a7'],
    [0.4, '#66bb6a'],
    [0.6, '#43a047'],
    [0.8, '#2e7d32'],
    [1, '#1b5e20']
];

// ============ MAP CHARTS ============

function createJobsMap() {
    const states = Object.keys(dashboardData.count_by_state);
    const counts = Object.values(dashboardData.count_by_state);
    
    const data = [{
        type: 'choropleth',
        locationmode: 'USA-states',
        locations: states,
        z: counts,
        text: states.map((state, i) => `${state}<br>Jobs: ${counts[i].toLocaleString()}`),
        hoverinfo: 'text',
        colorscale: blueScale,
        colorbar: {
            title: 'Job Count',
            thickness: 15,
            len: 0.7,
            tickformat: ','
        },
        marker: { line: { color: 'white', width: 1 } }
    }];

    const layout = {
        title: { text: 'Data Science & Analytics Jobs by State', font: { size: 20 } },
        geo: {
            scope: 'usa',
            showlakes: true,
            lakecolor: 'rgb(255,255,255)',
            projection: { type: 'albers usa' },
            bgcolor: '#f7fafc'
        },
        paper_bgcolor: '#ffffff',
        margin: { t: 60, b: 20, l: 20, r: 20 }
    };

    Plotly.newPlot('jobsMapChart', data, layout, { responsive: true, displayModeBar: false });
}

function createJobsMapInsights() {
    const states = Object.keys(dashboardData.count_by_state);
    const counts = Object.values(dashboardData.count_by_state);
    
    const data = [{
        type: 'choropleth',
        locationmode: 'USA-states',
        locations: states,
        z: counts,
        text: states.map((state, i) => `${state}<br>Jobs: ${counts[i].toLocaleString()}`),
        hoverinfo: 'text',
        colorscale: blueScale,
        colorbar: {
            title: 'Job Count',
            thickness: 15,
            len: 0.7,
            tickformat: ','
        },
        marker: { line: { color: 'white', width: 1 } }
    }];

    const layout = {
        geo: {
            scope: 'usa',
            showlakes: true,
            lakecolor: 'rgb(255,255,255)',
            projection: { type: 'albers usa' },
            bgcolor: '#f7fafc'
        },
        paper_bgcolor: '#ffffff',
        margin: { t: 20, b: 20, l: 20, r: 20 }
    };

    Plotly.newPlot('jobsMapChartInsights', data, layout, { responsive: true, displayModeBar: false });
}

function createSalaryMap() {
    const states = Object.keys(dashboardData.avg_salary_by_state);
    const salaries = Object.values(dashboardData.avg_salary_by_state);
    
    const data = [{
        type: 'choropleth',
        locationmode: 'USA-states',
        locations: states,
        z: salaries,
        text: states.map((state, i) => `${state}<br>Avg: $${salaries[i].toLocaleString()}`),
        hoverinfo: 'text',
        colorscale: greenScale,
        colorbar: {
            title: 'Avg Salary',
            thickness: 15,
            len: 0.7,
            tickformat: '$,.0f'
        },
        marker: { line: { color: 'white', width: 1 } }
    }];

    const layout = {
        title: { text: 'Average Data Science Salary by State', font: { size: 20 } },
        geo: {
            scope: 'usa',
            showlakes: true,
            lakecolor: 'rgb(255,255,255)',
            projection: { type: 'albers usa' },
            bgcolor: '#f7fafc'
        },
        paper_bgcolor: '#ffffff',
        margin: { t: 60, b: 20, l: 20, r: 20 }
    };

    Plotly.newPlot('salaryMapChart', data, layout, { responsive: true, displayModeBar: false });
}

function createSalaryMapInsights() {
    const states = Object.keys(dashboardData.avg_salary_by_state);
    const salaries = Object.values(dashboardData.avg_salary_by_state);
    
    const data = [{
        type: 'choropleth',
        locationmode: 'USA-states',
        locations: states,
        z: salaries,
        text: states.map((state, i) => `${state}<br>Avg: $${salaries[i].toLocaleString()}`),
        hoverinfo: 'text',
        colorscale: greenScale,
        colorbar: {
            title: 'Avg Salary',
            thickness: 15,
            len: 0.7,
            tickformat: '$,.0f'
        },
        marker: { line: { color: 'white', width: 1 } }
    }];

    const layout = {
        geo: {
            scope: 'usa',
            showlakes: true,
            lakecolor: 'rgb(255,255,255)',
            projection: { type: 'albers usa' },
            bgcolor: '#f7fafc'
        },
        paper_bgcolor: '#ffffff',
        margin: { t: 20, b: 20, l: 20, r: 20 }
    };

    Plotly.newPlot('salaryMapChartInsights', data, layout, { responsive: true, displayModeBar: false });
}

// ============ SOURCE CHARTS ============

function createSourceBreakdown() {
    const sources = Object.keys(dashboardData.count_by_source);
    const counts = Object.values(dashboardData.count_by_source);
    
    const data = [{
        values: counts,
        labels: sources,
        type: 'pie',
        hole: 0.4,
        marker: { colors: sources.map(s => sourceColors[s] || '#a0aec0') },
        textinfo: 'label+percent',
        textposition: 'outside',
        hovertemplate: '<b>%{label}</b><br>Jobs: %{value:,}<br>Percent: %{percent}<extra></extra>'
    }];

    const layout = {
        title: { text: 'Jobs by Data Source', font: { size: 18 } },
        showlegend: true,
        legend: { x: 0.5, y: -0.1, xanchor: 'center', orientation: 'h' },
        paper_bgcolor: '#ffffff',
        margin: { t: 60, b: 60, l: 40, r: 40 },
        annotations: [{
            text: `${dashboardData.total_jobs.toLocaleString()}<br>Total`,
            x: 0.5, y: 0.5,
            font: { size: 16, color: '#2d3748' },
            showarrow: false
        }]
    };

    Plotly.newPlot('sourceBreakdownChart', data, layout, { responsive: true, displayModeBar: false });
}

function createSalaryBySource() {
    const sources = Object.keys(dashboardData.avg_salary_by_source);
    const salaries = Object.values(dashboardData.avg_salary_by_source);

    const data = [{
        y: sources,
        x: salaries,
        type: 'bar',
        orientation: 'h',
        marker: { color: sources.map(s => sourceColors[s] || '#a0aec0') },
        text: salaries.map(s => `$${Math.round(s / 1000)}K`),
        textposition: 'outside',
        hovertemplate: '<b>%{y}</b><br>Avg Salary: $%{x:,.0f}<extra></extra>'
    }];

    const maxSalary = Math.max(...salaries);
    
    const layout = {
        title: { text: 'Average Salary by Source', font: { size: 18 } },
        xaxis: { 
            title: 'Average Salary ($)', 
            tickformat: '$,.0f',
            range: [0, maxSalary * 1.15]
        },
        yaxis: { automargin: true },
        paper_bgcolor: '#ffffff',
        plot_bgcolor: '#f7fafc',
        margin: { t: 60, b: 60, l: 100, r: 80 }
    };

    Plotly.newPlot('salaryBySourceChart', data, layout, { responsive: true, displayModeBar: false });
}

function createSalaryBySourceDetail() {
    const sources = Object.keys(dashboardData.avg_salary_by_source);
    const salaries = Object.values(dashboardData.avg_salary_by_source);

    const data = [{
        x: sources,
        y: salaries,
        type: 'bar',
        marker: { color: sources.map(s => sourceColors[s] || '#a0aec0') },
        text: salaries.map(s => `$${Math.round(s / 1000)}K`),
        textposition: 'outside',
        hovertemplate: '<b>%{x}</b><br>Avg Salary: $%{y:,.0f}<extra></extra>'
    }];

    const maxSalary = Math.max(...salaries);

    const layout = {
        title: { text: 'Average Salary by Source', font: { size: 18 } },
        xaxis: { title: 'Data Source' },
        yaxis: { 
            title: 'Average Salary ($)',
            tickformat: '$,.0f',
            range: [0, maxSalary * 1.15]
        },
        paper_bgcolor: '#ffffff',
        plot_bgcolor: '#f7fafc',
        showlegend: false
    };

    Plotly.newPlot('salaryBySourceDetailChart', data, layout, { responsive: true, displayModeBar: false });
}

// ============ STATE CHARTS ============

function createTopStatesChart() {
    const entries = Object.entries(dashboardData.count_by_state).slice(0, 10);
    const states = entries.map(e => e[0]);
    const counts = entries.map(e => e[1]);

    const data = [{
        x: states,
        y: counts,
        type: 'bar',
        marker: {
            color: counts,
            colorscale: blueScale
        },
        text: counts.map(c => c.toLocaleString()),
        textposition: 'outside',
        hovertemplate: '<b>%{x}</b><br>Jobs: %{y:,}<extra></extra>'
    }];

    const layout = {
        title: { text: 'Top 10 States by Job Count', font: { size: 18 } },
        xaxis: { title: 'State' },
        yaxis: { title: 'Number of Jobs', tickformat: ',' },
        paper_bgcolor: '#ffffff',
        plot_bgcolor: '#f7fafc',
        margin: { t: 60, b: 60, l: 80, r: 40 }
    };

    Plotly.newPlot('topStatesChart', data, layout, { responsive: true, displayModeBar: false });
}

// ============ JOB TYPE CHARTS ============

function createJobTypeChart() {
    const types = Object.keys(dashboardData.count_by_type);
    const counts = Object.values(dashboardData.count_by_type);

    const data = [{
        labels: types,
        values: counts,
        type: 'pie',
        hole: 0.4,
        marker: { colors: types.map(t => jobTypeColors[t] || '#a0aec0') },
        textinfo: 'label+percent',
        textposition: 'outside',
        hovertemplate: '<b>%{label}</b><br>Jobs: %{value:,}<br>Percent: %{percent}<extra></extra>'
    }];

    const layout = {
        title: { text: 'Job Type Distribution', font: { size: 18 } },
        showlegend: false,
        paper_bgcolor: '#ffffff',
        margin: { t: 60, b: 40, l: 40, r: 40 }
    };

    Plotly.newPlot('jobTypeChart', data, layout, { responsive: true, displayModeBar: false });
}

function createJobTypeBySource() {
    const sources = Object.keys(dashboardData.job_type_by_source);
    const allJobTypes = [...new Set(
        Object.values(dashboardData.job_type_by_source)
            .flatMap(obj => Object.keys(obj))
    )];

    const data = allJobTypes.map(type => ({
        x: sources,
        y: sources.map(source => dashboardData.job_type_by_source[source][type] || 0),
        name: type,
        type: 'bar',
        marker: { color: jobTypeColors[type] || '#a0aec0' }
    }));

    const layout = {
        title: { text: 'Job Types by Source', font: { size: 18 } },
        barmode: 'stack',
        xaxis: { title: 'Source' },
        yaxis: { title: 'Number of Jobs', tickformat: ',' },
        paper_bgcolor: '#ffffff',
        plot_bgcolor: '#f7fafc',
        legend: { x: 1.02, y: 1, xanchor: 'left' }
    };

    Plotly.newPlot('jobTypeBySourceChart', data, layout, { responsive: true, displayModeBar: false });
}

// ============ COMPANY/TITLE CHARTS ============

function createTopCompaniesChart() {
    const entries = Object.entries(dashboardData.count_by_company);
    const companies = entries.map(e => e[0]).reverse();
    const counts = entries.map(e => e[1]).reverse();

    const data = [{
        y: companies,
        x: counts,
        type: 'bar',
        orientation: 'h',
        marker: {
            color: counts,
            colorscale: blueScale
        },
        text: counts.map(c => c.toLocaleString()),
        textposition: 'outside',
        hovertemplate: '<b>%{y}</b><br>Jobs: %{x:,}<extra></extra>'
    }];

    const maxCount = Math.max(...counts);

    const layout = {
        title: { text: 'Top Hiring Organizations', font: { size: 18 } },
        xaxis: { 
            title: 'Number of Postings',
            range: [0, maxCount * 1.2]
        },
        yaxis: { automargin: true },
        paper_bgcolor: '#ffffff',
        plot_bgcolor: '#f7fafc',
        margin: { l: 250, r: 80, t: 60, b: 60 }
    };

    Plotly.newPlot('topCompaniesChart', data, layout, { responsive: true, displayModeBar: false });
}

function createKeywordChart() {
    const entries = Object.entries(dashboardData.count_by_title);
    const titles = entries.map(e => e[0]).reverse();
    const counts = entries.map(e => e[1]).reverse();

    const data = [{
        y: titles,
        x: counts,
        type: 'bar',
        orientation: 'h',
        marker: {
            color: counts,
            colorscale: 'Viridis'
        },
        text: counts.map(c => c.toLocaleString()),
        textposition: 'outside',
        hovertemplate: '<b>%{y}</b><br>Jobs: %{x:,}<extra></extra>'
    }];

    const maxCount = Math.max(...counts);

    const layout = {
        title: { text: 'Top Job Titles', font: { size: 18 } },
        xaxis: { 
            title: 'Number of Postings',
            range: [0, maxCount * 1.2]
        },
        yaxis: { automargin: true },
        paper_bgcolor: '#ffffff',
        plot_bgcolor: '#f7fafc',
        margin: { l: 300, r: 80, t: 60, b: 60 }
    };

    Plotly.newPlot('keywordChart', data, layout, { responsive: true, displayModeBar: false });
}

// ============ PIPELINE CHARTS ============

function createPipelineFlow() {
    const sourceCounts = dashboardData.count_by_source;
    const total = dashboardData.total_jobs;
    
    const data = [{
        type: 'sankey',
        orientation: 'h',
        node: {
            pad: 15,
            thickness: 20,
            line: { color: 'black', width: 0.5 },
            label: [
                'Adzuna API', 'USAJobs API', 'Jooble API',
                'Raw Jobs Table', 'Data Cleaning', 'Cleaned Jobs Table',
                'Analysis Ready'
            ],
            color: ['#667eea', '#764ba2', '#f5576c', '#feca57', '#43e97b', '#00f2fe', '#38ef7d']
        },
        link: {
            source: [0, 1, 2, 3, 4, 5],
            target: [3, 3, 3, 4, 5, 6],
            value: [
                sourceCounts['Adzuna'] || 0,
                sourceCounts['USAJobs'] || 0,
                sourceCounts['Jooble'] || 0,
                total, total, total
            ],
            color: [
                'rgba(102,126,234,0.4)', 'rgba(118,75,162,0.4)', 'rgba(245,87,108,0.4)',
                'rgba(254,202,87,0.4)', 'rgba(67,233,123,0.4)', 'rgba(0,242,254,0.4)'
            ]
        }
    }];

    const layout = {
        title: { text: 'Data Pipeline Flow', font: { size: 20 } },
        font: { size: 12 },
        paper_bgcolor: '#ffffff',
        margin: { t: 60, b: 40, l: 40, r: 40 }
    };

    Plotly.newPlot('pipelineFlowChart', data, layout, { responsive: true, displayModeBar: false });
}

function createDagStructure() {
    const tasks = [
        'collect_adzuna',
        'collect_usajobs', 
        'collect_jooble',
        'load_raw_to_db',
        'cleanup_json_files',
        'clean_data',
        'verify_cleaned_data',
        'generate_summary'
    ];
    
    const startTimes = [0, 0, 0, 17, 18, 19, 21, 22];
    const durations = [17, 17, 17, 1, 1, 2, 1, 0.5];
    
    const colors = [
        '#667eea', '#764ba2', '#f5576c',
        '#feca57', '#a55eea', '#43e97b', '#00f2fe', '#38ef7d'
    ];

    const data = tasks.map((task, i) => ({
        x: [startTimes[i], startTimes[i] + durations[i]],
        y: [task, task],
        mode: 'lines',
        line: { color: colors[i], width: 25 },
        name: task,
        hovertemplate: `<b>${task}</b><br>Start: ${startTimes[i]} min<br>Duration: ${durations[i]} min<extra></extra>`
    }));

    const layout = {
        title: { text: 'DAG Execution Timeline (~23 minutes total)', font: { size: 18 } },
        xaxis: {
            title: 'Minutes from Start',
            range: [-1, 25],
            tickvals: [0, 5, 10, 15, 17, 20, 23]
        },
        yaxis: {
            automargin: true,
            categoryorder: 'array',
            categoryarray: [...tasks].reverse()
        },
        showlegend: false,
        paper_bgcolor: '#ffffff',
        plot_bgcolor: '#f7fafc',
        margin: { t: 60, b: 60, l: 150, r: 40 },
        shapes: [{
            type: 'line',
            x0: 17,
            x1: 17,
            y0: -0.5,
            y1: 7.5,
            line: { color: '#e2e8f0', width: 2, dash: 'dash' }
        }]
    };

    Plotly.newPlot('dagStructureChart', data, layout, { responsive: true, displayModeBar: false });
}

// ============ INITIALIZE ============

document.addEventListener('DOMContentLoaded', initDashboard);
