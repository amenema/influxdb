// Libraries
import React, {PureComponent} from 'react'
import {connect} from 'react-redux'

// Components
import TimeMachineFluxEditor from 'src/timeMachine/components/TimeMachineFluxEditor'
import CSVExportButton from 'src/shared/components/CSVExportButton'
import TimeMachineQueriesSwitcher from 'src/timeMachine/components/QueriesSwitcher'
import TimeMachineRefreshDropdown from 'src/timeMachine/components/RefreshDropdown'
import TimeRangeDropdown from 'src/shared/components/TimeRangeDropdown'
import TimeMachineQueryTab from 'src/timeMachine/components/QueryTab'
import TimeMachineQueryBuilder from 'src/timeMachine/components/QueryBuilder'
import SubmitQueryButton from 'src/timeMachine/components/SubmitQueryButton'
import RawDataToggle from 'src/timeMachine/components/RawDataToggle'
import {
  Button,
  IconFont,
  ButtonShape,
  ComponentSize,
  ComponentColor,
  ComponentSpacer,
  FlexDirection,
  JustifyContent,
} from '@influxdata/clockface'

// Actions
import {addQuery, setAutoRefresh} from 'src/timeMachine/actions'
import {setTimeRange} from 'src/timeMachine/actions'

// Utils
import {getActiveTimeMachine, getActiveQuery} from 'src/timeMachine/selectors'

// Types
import {
  AppState,
  DashboardQuery,
  QueryEditMode,
  TimeRange,
  AutoRefresh,
  AutoRefreshStatus,
} from 'src/types'
import {DashboardDraftQuery} from 'src/types/dashboards'

interface StateProps {
  activeQuery: DashboardQuery
  draftQueries: DashboardDraftQuery[]
  timeRange: TimeRange
  autoRefresh: AutoRefresh
}

interface DispatchProps {
  onAddQuery: typeof addQuery
  onSetTimeRange: typeof setTimeRange
  onSetAutoRefresh: typeof setAutoRefresh
}

type Props = StateProps & DispatchProps

class TimeMachineQueries extends PureComponent<Props> {
  public render() {
    const {draftQueries, onAddQuery, timeRange} = this.props

    return (
      <div className="time-machine-queries">
        <div className="time-machine-queries--controls">
          <div className="time-machine-queries--tabs">
            {draftQueries.map((query, queryIndex) => (
              <TimeMachineQueryTab
                key={queryIndex}
                queryIndex={queryIndex}
                query={query}
              />
            ))}
            <Button
              customClass="time-machine-queries--new"
              shape={ButtonShape.Square}
              icon={IconFont.PlusSkinny}
              size={ComponentSize.ExtraSmall}
              color={ComponentColor.Default}
              onClick={onAddQuery}
            />
          </div>
          <div className="time-machine-queries--buttons">
            <ComponentSpacer
              direction={FlexDirection.Row}
              justifyContent={JustifyContent.FlexEnd}
              margin={ComponentSize.Small}
            >
              <RawDataToggle />
              <CSVExportButton />
              <TimeMachineRefreshDropdown />
              <TimeRangeDropdown
                timeRange={timeRange}
                onSetTimeRange={this.handleSetTimeRange}
              />
              <TimeMachineQueriesSwitcher />
              <SubmitQueryButton />
            </ComponentSpacer>
          </div>
        </div>
        <div className="time-machine-queries--body">{this.queryEditor}</div>
      </div>
    )
  }

  private handleSetTimeRange = (
    timeRange: TimeRange,
    absoluteTimeRange?: boolean
  ) => {
    const {autoRefresh, onSetAutoRefresh, onSetTimeRange} = this.props

    if (absoluteTimeRange) {
      onSetAutoRefresh({...autoRefresh, status: AutoRefreshStatus.Disabled})
    } else if (autoRefresh.status === AutoRefreshStatus.Disabled) {
      if (autoRefresh.interval === 0) {
        onSetAutoRefresh({...autoRefresh, status: AutoRefreshStatus.Paused})
      } else {
        onSetAutoRefresh({...autoRefresh, status: AutoRefreshStatus.Active})
      }
    }

    onSetTimeRange(timeRange)
  }

  private get queryEditor(): JSX.Element {
    const {activeQuery} = this.props

    if (activeQuery.editMode === QueryEditMode.Builder) {
      return <TimeMachineQueryBuilder />
    } else if (activeQuery.editMode === QueryEditMode.Advanced) {
      return <TimeMachineFluxEditor />
    } else {
      return null
    }
  }
}

const mstp = (state: AppState) => {
  const {draftQueries, timeRange, autoRefresh} = getActiveTimeMachine(state)

  const activeQuery = getActiveQuery(state)

  return {timeRange, activeQuery, draftQueries, autoRefresh}
}

const mdtp = {
  onAddQuery: addQuery,
  onSetTimeRange: setTimeRange,
  onSetAutoRefresh: setAutoRefresh,
}

export default connect<StateProps, DispatchProps>(
  mstp,
  mdtp
)(TimeMachineQueries)
