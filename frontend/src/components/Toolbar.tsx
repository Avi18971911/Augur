import {NavLink} from "react-router-dom";
import routes from "../config/routes.ts";

function Toolbar() {
    return (
        <nav>
            <ul style={{ display: 'flex', listStyle: 'none', gap: '1rem', padding: 0 }}>
                {Object.entries(routes).map(([key, route]) => (
                    <li key={key}>
                        <NavLink
                            to={route.path}
                            className={({ isActive }) =>
                                isActive ? 'nav-link active' : 'nav-link'
                            }
                        >
                            {route.label}
                        </NavLink>
                    </li>
                ))}
            </ul>
        </nav>
    );
}

export default Toolbar