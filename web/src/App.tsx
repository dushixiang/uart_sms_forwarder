import {BrowserRouter, Route, Routes} from 'react-router-dom';
import {QueryProvider} from './providers/QueryProvider';
import {ProtectedRoute} from './components/ProtectedRoute';
import Layout from './components/Layout';
import Login from './pages/Login';
import OIDCCallback from './pages/OIDCCallback';
import Dashboard from './pages/Dashboard';
import Messages from './pages/Messages';
import SerialControl from './pages/SerialControl';
import NotificationChannels from './pages/NotificationChannels';
import ScheduledTasksConfig from './pages/ScheduledTasksConfig';
import AutoFlymodeSettings from './pages/AutoFlymodeSettings';
import {Toaster} from "@/components/ui/sonner.tsx";

function App() {
    return (
        <QueryProvider>
            <BrowserRouter>
                <Routes>
                    {/* 公开路由 */}
                    <Route path="/login" element={<Login/>}/>
                    <Route path="/oidc/callback" element={<OIDCCallback/>}/>

                    {/* 受保护的路由 */}
                    <Route
                        path="/"
                        element={
                            <ProtectedRoute>
                                <Layout/>
                            </ProtectedRoute>
                        }
                    >
                        <Route index element={<Dashboard/>}/>
                        <Route path="messages" element={<Messages/>}/>
                        <Route path="serial" element={<SerialControl/>}/>
                        <Route path="notifications" element={<NotificationChannels/>}/>
                        <Route path="scheduled-tasks" element={<ScheduledTasksConfig/>}/>
                        <Route path="auto-flymode" element={<AutoFlymodeSettings/>}/>
                    </Route>
                </Routes>
            </BrowserRouter>

            <Toaster/>
        </QueryProvider>
    );
}

export default App;
