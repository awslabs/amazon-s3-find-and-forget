import React, { useState } from "react";
import { Authenticator, Greetings, SignUp } from "aws-amplify-react";

import AppLayout from "./components/AppLayout";
import Header from "./components/Header";
import {
  DashboardHelp,
  DataMappersHelp,
  DeletionJobDetailsHelp,
  DeletionJobsHelp,
  DeletionQueueHelp,
  NewDataMapperHelp,
  NewDeletionQueueMatchHelp,
} from "./components/help";
import {
  DashboardPage,
  DataMappersPage,
  DeletionJobPage,
  DeletionJobsPage,
  DeletionQueuePage,
  NewDataMapperPage,
  NewDeletionQueueMatchPage,
} from "./components/pages";
import gateway from "./utils/gateway";

export default () => {
  const [authState, setAuthState] = useState(undefined);
  const [currentPage, setCurrentPage] = useState(0);
  const [selectedJobId, selectJobId] = useState(undefined);

  const goToJobDetails = (jobId) => {
    selectJobId(jobId);
    setCurrentPage(6);
  };

  const pages = [
    {
      title: "Dashboard",
      page: (
        <DashboardPage
          gateway={gateway}
          goToJobDetails={goToJobDetails}
          goToPage={setCurrentPage}
        />
      ),
      help: <DashboardHelp />,
    },
    {
      title: "Data Mappers",
      page: <DataMappersPage gateway={gateway} onPageChange={setCurrentPage} />,
      help: <DataMappersHelp />,
    },
    {
      title: "Deletion Queue",
      page: (
        <DeletionQueuePage gateway={gateway} onPageChange={setCurrentPage} />
      ),
      help: <DeletionQueueHelp />,
    },
    {
      title: "Deletion Jobs",
      page: (
        <DeletionJobsPage gateway={gateway} goToJobDetails={goToJobDetails} />
      ),
      help: <DeletionJobsHelp />,
    },
    {
      title: "Create Data Mapper",
      page: (
        <NewDataMapperPage
          gateway={gateway}
          goToDataMappers={() => setCurrentPage(1)}
        />
      ),
      help: <NewDataMapperHelp />,
      parent: 1,
    },
    {
      title: "Add item to the Deletion Queue",
      page: (
        <NewDeletionQueueMatchPage
          gateway={gateway}
          goToDeletionQueue={() => setCurrentPage(2)}
        />
      ),
      help: <NewDeletionQueueMatchHelp />,
      parent: 2,
    },
    {
      title: selectedJobId || "Deletion Job details",
      page: (
        <DeletionJobPage
          gateway={gateway}
          goToJobsList={() => setCurrentPage(3)}
          jobId={selectedJobId}
        />
      ),
      help: <DeletionJobDetailsHelp />,
      parent: 3,
    },
  ];

  const classNames = ["App"];
  const signedIn = authState === "signedIn";
  if (!signedIn) classNames.push("amplify-auth");

  return (
    <div className={classNames.join(" ")}>
      <Header signedIn={signedIn} />
      {signedIn ? (
        <AppLayout
          currentPage={currentPage}
          onMenuClick={setCurrentPage}
          pages={pages}
        />
      ) : (
        <Authenticator
          onStateChange={(s) => setAuthState(s)}
          hide={[Greetings, SignUp]}
        />
      )}
    </div>
  );
};
